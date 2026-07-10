BUMP := $(filter fix minor major,$(MAKECMDGOALS))

.PHONY: install test typecheck build smoke check publish fix minor major

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

publish:
	@test "$(words $(BUMP))" -eq 1 || { echo "usage: make publish fix|minor|major"; exit 2; }
	bun pm whoami
	$(MAKE) check
	@BUMP="$(BUMP)" bun -e 'const p = await Bun.file("package.json").json(); if (!/^\d+\.\d+\.\d+$$/.test(p.version)) throw new Error("version must be x.y.z"); const v = p.version.split(".").map(Number); const i = { major: 0, minor: 1, fix: 2 }[process.env.BUMP]; v[i]++; for (let j = i + 1; j < 3; j++) v[j] = 0; p.version = v.join("."); await Bun.write("package.json", JSON.stringify(p, null, 2) + "\n"); console.log(`version $${p.version}`);'
	bun publish --dry-run --access public
	bun publish --access public

fix minor major:
	@:
