SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONTEXT=$SCRIPT_DIR

docker buildx build $CONTEXT \
  --file gateway.Dockerfile \
  --tag registry.gitlab.com/huinnoaim/sp/iomt/tb-gateway \
  --push \
  --platform linux/arm64,linux/amd64
