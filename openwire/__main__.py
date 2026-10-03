"""Allow ``python -m openwire`` as an alternative to the ``openwire`` script."""

from .cli import main

raise SystemExit(main())
