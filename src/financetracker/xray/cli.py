"""Console entrypoint for the Xray sidecar runtime."""

from .entrypoint import main


if __name__ == "__main__":
    raise SystemExit(main())
