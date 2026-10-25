# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run dev      # start dev server (hot-reload)
npm run build    # build static site to .vitepress/dist/
npm run preview  # preview the production build locally
```

## Architecture

This is a **VitePress documentation site** for the [kafka-bus](https://github.com/kafka-bus/kafka-bus) PHP library ecosystem. There is no application code — only Markdown content and a single VitePress config.

### Versioned, bilingual structure

Docs are split by language first (VitePress locales), then by major version:

| Locale  | Path              | Language | Versions        |
|---------|-------------------|----------|-----------------|
| English | `docs/{version}/` | `en-US`  | `1.x`, `2.x`    |
| Russian | `ru/docs/{version}/` | `ru-RU` | `1.x`, `2.x` |

- `2.x` is the current version — new content goes here. `1.x` is frozen: fix only factual errors.
- Every page exists in both locales; when adding, removing, or renaming a page, mirror it in both locale trees of the same version.
- Internal links are absolute and include the version: `/docs/2.x/...`, `/ru/docs/2.x/...`. Never link across versions except from the upgrade guide or the outdated-version banner.
- Pages under `*/1.x/` automatically show a "you're reading 1.x" banner (`.vitepress/theme/OutdatedVersionBanner.vue`).
- `docs/2.x/upgrade.md` (and its RU twin) is the 1.x → 2.x migration guide; keep it in sync when a breaking change is documented.

### Navigation and sidebar

Nav and sidebars live in `.vitepress/config.ts`. Sidebars are generated per version by `enSidebar(version)` and `ruSidebar(version)` — pages that exist only in one version (e.g. `consumer/workers`, `components/metadata`, `upgrade` are 2.x-only) are added conditionally there. Adding a new `.md` file does **not** surface it — add it to both generator functions. The `LATEST` constant controls where nav links point.

### Content sections (2.x)

- **`docs/2.x/`** — core (`kafka-bus/core`): installation, upgrade, configuration, architecture, topics, `producer/`, `consumer/` (handlers, routes, workers from `kafka-bus/worker`, pipeline, testing)
- **`docs/2.x/components/`** — `messages` (`kafka-bus/messages`), `commiter` (`kafka-bus/commiter`), `metadata` (`kafka-bus/metadata`)
- **`docs/2.x/laravel/`** — Laravel bridge (`kafka-bus/laravel-bridge`): config, Artisan commands, facade, testing

### Source of truth for package content

This repo contains no PHP code — never invent or guess API details, method signatures, config keys, or Artisan command names from memory. The actual package sources are vendored in as git submodules under `.packages/` (excluded from the VitePress build since the dir starts with `.`):

| Package                    | Submodule path                          |
|----------------------------|-----------------------------------------|
| `kafka-bus/core`           | `.packages/kafka-bus/packages/core`     |
| `kafka-bus/worker`         | `.packages/kafka-bus/packages/worker`   |
| `kafka-bus/metadata`       | `.packages/kafka-bus/packages/metadata` |
| `kafka-bus/messages`       | `.packages/kafka-bus/packages/messages` |
| `kafka-bus/commiter`       | `.packages/kafka-bus/packages/commiter` |
| `kafka-bus/laravel-bridge` | `.packages/laravel-bridge`              |

Both submodule repos have `1.x` and `2.x` branches matching the doc versions. Check out the branch that matches the version you're documenting (`git -C .packages/kafka-bus checkout 2.x`); to compare versions, diff the tags (e.g. `git -C .packages/kafka-bus diff v1.3.0 v2.1.0 -- packages`).

Before writing or updating a docs page, read the relevant package's `README.md`/`CLAUDE.md`, `src/`, and `tests/` (tests double as real usage examples) inside its submodule path — do not rely on what this site's docs already say, since they can lag behind the code.

If `.packages/*` directories are empty, initialize them first: `git submodule update --init --recursive`. To pull in upstream changes later: `git submodule update --remote --merge`.