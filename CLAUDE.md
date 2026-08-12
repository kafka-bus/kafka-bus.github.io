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

### Bilingual structure

Every page exists in two locales that must stay in sync:

| Locale  | Root path  | Language |
|---------|------------|----------|
| English | `docs/`    | `en-US`  |
| Russian | `ru/docs/` | `ru-RU`  |

When adding, removing, or renaming a page, mirror the change in both locale trees.

### Navigation and sidebar

All nav items and sidebar entries are declared in `.vitepress/config.ts` under `locales.root.themeConfig` (EN) and `locales.ru.themeConfig` (RU). Adding a new `.md` file does **not** automatically surface it — you must add a matching entry to both sidebar objects in the config.

### Content sections

The documentation covers four packages:

- **`docs/`** — core library (`kafka-bus/core`): installation, configuration, architecture, topics, producer, consumer
- **`docs/laravel/`** — Laravel bridge (`kafka-bus/laravel-bridge`): Artisan commands, facade, testing
- **`docs/components/messages`** — typed Payload / DomainMessage (`kafka-bus/messages`)
- **`docs/components/commiter`** — idempotent processing middleware (`kafka-bus/commiter`)

### Source of truth for package content

This repo contains no PHP code — never invent or guess API details, method signatures, config keys, or Artisan command names from memory. The actual package sources are vendored in as git submodules under `.packages/` (excluded from the VitePress build since the dir starts with `.`):

| Package | Submodule path |
|---|---|
| `kafka-bus/core` | `.packages/kafka-bus/packages/core` |
| `kafka-bus/messages` | `.packages/kafka-bus/packages/messages` |
| `kafka-bus/commiter` | `.packages/kafka-bus/packages/commiter` |
| `kafka-bus/laravel-bridge` | `.packages/laravel-bridge` |

Before writing or updating a docs page, read the relevant package's `README.md`/`CLAUDE.md`, `src/`, and `tests/` (tests double as real usage examples) inside its submodule path — do not rely on what this site's docs already say, since they can lag behind the code.

If `.packages/*` directories are empty, initialize them first: `git submodule update --init --recursive`. To pull in upstream changes later: `git submodule update --remote --merge`.