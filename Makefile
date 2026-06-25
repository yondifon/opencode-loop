.PHONY: install test typecheck build smoke check

install: build
	mkdir -p ~/.config/opencode/plugins
	cp dist/index.js ~/.config/opencode/plugins/opencode-loop.js
	@echo "Installed to ~/.config/opencode/plugins/opencode-loop.js"
	@echo "Restart OpenCode to load the plugin."

test:
	bun test

typecheck:
	bun run typecheck

build:
	bun run build

smoke: build
	bun -e 'const m = await import("./dist/index.js"); if (typeof m.default !== "function") throw new Error("default export bad");'

check: typecheck test smoke
