# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run docs:dev      # start dev server (hot-reload)
npm run docs:build    # build static site to .vitepress/dist/
npm run docs:preview  # preview the production build locally
```

## Architecture

This is a **VitePress documentation site** for the [kafka-bus](https://github.com/kafka-bus/kafka-bus) PHP library ecosystem. There is no application code — only Markdown content and a single VitePress config.

### Bilingual structure

Every page exists in two locales that must stay in sync:

| Locale | Root path | Language |
|--------|-----------|----------|
| English | `docs/` | `en-US` |
| Russian | `ru/docs/` | `ru-RU` |

When adding, removing, or renaming a page, mirror the change in both locale trees.

### Navigation and sidebar

All nav items and sidebar entries are declared in `.vitepress/config.ts` under `locales.root.themeConfig` (EN) and `locales.ru.themeConfig` (RU). Adding a new `.md` file does **not** automatically surface it — you must add a matching entry to both sidebar objects in the config.

### Content sections

The documentation covers four packages:

- **`docs/`** — core library (`kafka-bus/core`): installation, configuration, architecture, topics, producer, consumer
- **`docs/laravel/`** — Laravel bridge (`kafka-bus/laravel-bridge`): Artisan commands, facade, testing
- **`docs/components/messages`** — typed Payload / DomainMessage (`kafka-bus/messages`)
- **`docs/components/commiter`** — idempotent processing middleware (`kafka-bus/commiter`)