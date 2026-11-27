#!/usr/bin/env bash

# Build a Cassandra image from a local build and push it to a private ECR
# repository, from which every node of the deployment pulls it (pull_images
# logs in first; see ecr_login in utils.sh).  Unlike `./deploy.sh push-image`,
# only the layers that changed cross the wire, once, and a node that is
# replaced later (./deploy.sh repair, run_resilient.sh) gets the image too.
#
# Usage: tools/build_cassandra_image.sh <cassandra-checkout | bin-tarball> [options]
#        tools/build_cassandra_image.sh --prune=N [--repo=NAME] [--region=REGION] [--dry-run]
#
#   <cassandra-checkout>  a checkout where `ant artifacts` has run; the newest
#                         build/apache-cassandra-*-bin.tar.gz is used
#   <bin-tarball>         or that tarball itself
#
#   --use[=accord|normal] point exp.config's accord_cassandra_image (default)
#                         or normal_cassandra_image at the pushed image
#   --tag=TAG             image tag (default: <commit>[-dirty]-<tarball hash>)
#   --repo=NAME           ECR repository (default: bench/cassandra-accord; the
#                         IAM policy only covers bench/*)
#   --region=REGION       ECR region (default: BENCH_ECR_REGION, else the aws
#                         CLI's region, else us-east-1)
#   --no-push             build only
#   --prune=N             after any build and push, delete from the repository
#                         all but the N most recently pushed images; an image
#                         exp.config uses, or the one just pushed, is never
#                         deleted (and does not count towards N).  Without a
#                         checkout or tarball, only prunes.
#   --dry-run             with --prune, only list what would be deleted
#
# The image is built for linux/amd64 whatever this machine is, as the EC2
# instances are x86: an arm64 image built by default on Apple Silicon would
# fail there with "exec format error".

set -o pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "${ROOT}/utils.sh"

DOCKERFILE_DIR="${ROOT}/cassandra-docker-library/5.1-accord"

usage() { awk 'NR > 2 && /^#/ { sub(/^# ?/, ""); print; next } NR > 2 { exit }' "$0"; }

src="" use="" tag="" repo="bench/cassandra-accord" region="${BENCH_ECR_REGION:-}" push=1 prune="" dry_run=0
for arg in "$@"; do
    case "${arg}" in
        --use) use=accord ;;
        --use=*) use="${arg#*=}" ;;
        --tag=*) tag="${arg#*=}" ;;
        --repo=*) repo="${arg#*=}" ;;
        --region=*) region="${arg#*=}" ;;
        --no-push) push=0 ;;
        --prune=*) prune="${arg#*=}" ;;
        --dry-run) dry_run=1 ;;
        -h|--help) usage; exit 0 ;;
        -*) error "Unknown option: ${arg}"; usage; exit 1 ;;
        *) [ -z "${src}" ] || { error "Only one checkout or tarball"; exit 1; }; src="${arg}" ;;
    esac
done
[ -n "${src}" ] || [ -n "${prune}" ] || { usage; exit 1; }
case "${use}" in ""|accord|normal) ;; *) error "--use takes accord or normal"; exit 1 ;; esac
if [ -n "${prune}" ] && ! [[ "${prune}" =~ ^[1-9][0-9]*$ ]]; then
    error "--prune takes a number of images to keep, at least 1"
    exit 1
fi
if [ -n "${prune}" ] && [ "${push}" -eq 0 ]; then
    error "--prune works on the ECR repository, which --no-push leaves alone"
    exit 1
fi
if [ -z "${src}" ] && { [ -n "${use}" ] || [ -n "${tag}" ]; }; then
    error "--use and --tag need a checkout or tarball to build"
    exit 1
fi

# The registry, for pushing and pruning.
if [ "${push}" -eq 1 ]; then
    account=$(aws sts get-caller-identity --query Account --output text) || {
        error "No working aws CLI credentials"; exit 1; }
    region=${region:-$(aws configure get region 2>/dev/null)}
    region=${region:-us-east-1}
    registry="${account}.dkr.ecr.${region}.amazonaws.com"
fi

mtime() { python3 -c 'import os, sys; print(int(os.path.getmtime(sys.argv[1])))' "$1"; }

# build_and_push: builds the image of ${src} as ${image}, pushes it unless
# --no-push, and points exp.config at it with --use.
build_and_push() {
    local checkout="" tarball hash commit dirty ctx

    if [ -d "${src}" ]; then
        checkout="${src}"
        tarball=$(ls -t "${src}"/build/apache-cassandra-*-bin.tar.gz 2>/dev/null | head -1)
        if [ -z "${tarball}" ]; then
            error "No build/apache-cassandra-*-bin.tar.gz in ${src}; run there:"
            error "  ant artifacts -Dcheck.skip=true -Dant.gen-doc.skip=true -Dno-javadoc=true"
            exit 1
        fi
    elif [ -f "${src}" ]; then
        tarball="${src}"
    else
        error "${src}: no such checkout or tarball"
        exit 1
    fi

    hash=$( { command -v sha256sum >/dev/null && sha256sum "${tarball}" || shasum -a 256 "${tarball}"; } | cut -c1-8)
    if [ -z "${tag}" ]; then
        tag="${hash}"
        if [ -n "${checkout}" ] && git -C "${checkout}" rev-parse --git-dir >/dev/null 2>&1; then
            commit=$(git -C "${checkout}" rev-parse --short=10 HEAD)
            dirty=""
            git -C "${checkout}" diff --quiet HEAD 2>/dev/null || dirty="-dirty"
            tag="${commit}${dirty}-${hash}"
            # The tag names the commit, so a tarball older than it was built
            # from something else.
            if [ "$(mtime "${tarball}")" -lt "$(git -C "${checkout}" log -1 --format=%ct)" ]; then
                error "Warning: ${tarball} is older than HEAD (${commit}); was 'ant artifacts' re-run?"
            fi
        fi
    fi

    # A local name with --no-push, the ECR one otherwise.
    if [ "${push}" -eq 1 ]; then
        image="${registry}/${repo}:${tag}"
    else
        image="${repo}:${tag}"
    fi

    ctx=$(mktemp -d)
    # shellcheck disable=SC2064 -- ctx is fixed now, expand it immediately
    trap "rm -rf '${ctx}'" EXIT
    cp "${DOCKERFILE_DIR}/Dockerfile" "${DOCKERFILE_DIR}/docker-entrypoint.sh" "${ctx}/" || exit 1
    cp "${tarball}" "${ctx}/cassandra-bin.tgz" || exit 1

    log "Building ${image} (linux/amd64) from ${tarball}"
    docker build --platform linux/amd64 -t "${image}" "${ctx}" || { error "docker build failed"; exit 1; }

    if [ "${push}" -eq 1 ]; then
        if ! aws ecr describe-repositories --region "${region}" --repository-names "${repo}" >/dev/null 2>&1; then
            log "Creating private ECR repository ${repo} in ${region}"
            aws ecr create-repository --region "${region}" --repository-name "${repo}" >/dev/null || {
                error "Could not create ECR repository ${repo} (is infra/aws/iam-policy.json applied?)"; exit 1; }
        fi
        ecr_login "${image}" || exit 1
        log "Pushing ${image}"
        docker push "${image}" || { error "docker push failed"; exit 1; }
    fi

    if [ -n "${use}" ]; then
        "${ROOT}/deploy.sh" use-image "${use}" "${image}" || exit 1
    fi

    log "Image: ${image}"
    [ -n "${use}" ] || log "Use it with: ./deploy.sh use-image accord ${image}"
}

# prune_repository <keep>: deletes all but the <keep> most recently pushed
# images of the repository, sparing those exp.config names (by tag or by
# digest) and the one just pushed, which do not count towards <keep>.
prune_repository() {
    local keep=$1 listing protected doomed
    local -a ids=()

    if ! listing=$(aws ecr describe-images --region "${region}" --repository-name "${repo}" --output json 2>&1); then
        if printf '%s' "${listing}" | grep -q RepositoryNotFoundException; then
            log "No ECR repository ${repo} in ${region}: nothing to prune"
            return 0
        fi
        error "Could not list the images of ${repo} in ${region}: ${listing}"
        return 1
    fi

    # What must survive: every *_image of exp.config in this repository, and
    # the image just built, as tags or as digests.
    protected=$(
        {
            sed -n 's/^[^#]*_image=//p' "${CONFIG_FILE}" | tr -d ' \t\r'
            [ -n "${image:-}" ] && echo "${image}"
        } | awk -v r="${registry}/${repo}" '
            index($0, r ":") == 1 { print "tag " substr($0, length(r) + 2) }
            index($0, r "@") == 1 { print "digest " substr($0, length(r) + 2) }'
    )

    # One line per image to delete: "<digest> <tags>", oldest first.
    doomed=$(python3 -c '
import json, sys
images = json.loads(sys.argv[1]).get("imageDetails", [])
keep = int(sys.argv[2])
protected = set(l for l in sys.argv[3].splitlines() if l)
def spared(i):
    return ("digest " + i["imageDigest"]) in protected or \
           any(("tag " + t) in protected for t in i.get("imageTags", []))
candidates = sorted((i for i in images if not spared(i)),
                    key=lambda i: i["imagePushedAt"], reverse=True)
for i in reversed(candidates[keep:]):
    print(i["imageDigest"], ",".join(i.get("imageTags", [])) or "<untagged>")
' "${listing}" "${keep}" "${protected}") || { error "Could not work out what to prune"; return 1; }

    if [ -z "${doomed}" ]; then
        log "Nothing to prune in ${repo}: at most ${keep} image(s) besides those in use"
        return 0
    fi

    local digest tags
    while read -r digest tags; do
        log "$([ "${dry_run}" -eq 1 ] && echo 'Would delete' || echo 'Deleting') ${repo}:${tags} (${digest})"
        ids+=("imageDigest=${digest}")
    done <<< "${doomed}"
    [ "${dry_run}" -eq 1 ] && return 0

    # batch-delete-image takes at most 100 images per call.
    local i out
    for (( i = 0; i < ${#ids[@]}; i += 100 )); do
        if ! out=$(aws ecr batch-delete-image --region "${region}" --repository-name "${repo}" \
                --image-ids "${ids[@]:i:100}" --query 'failures' --output text 2>&1) || \
           { [ -n "${out}" ] && [ "${out}" != "None" ]; }; then
            error "Could not delete every image: ${out}"
            return 1
        fi
    done
    log "Pruned ${#ids[@]} image(s) from ${repo}"
}

image=""
[ -n "${src}" ] && build_and_push
[ -n "${prune}" ] && { prune_repository "${prune}" || exit 1; }
exit 0
