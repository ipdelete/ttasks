"""Entry point: start the kanban server on 127.0.0.1:8000."""

from kanban.server import serve


def main() -> None:
    """Launch the kanban server bound to loopback."""
    serve(host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
