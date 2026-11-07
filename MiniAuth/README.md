# MiniAuth

Paper authentication plugin with `/register`, `/login`, and `/logout`. Player messages are configurable in `messages.yml`; account storage supports local SQLite or MySQL.

## Requirements

- Paper 1.21.x (compiled against Paper API 1.21.1)
- Java 21 or newer to run the plugin; built for Java 21 bytecode and intended for Java 21–25 runtimes. Verify Java and Paper compatibility for the exact server build you deploy.
- JDK 21 and Maven to build

## Build

```bash
mvn clean package
```

The shaded plugin JAR is created in `target/`.

## Install

1. Copy the built JAR into the Paper server's `plugins/` directory.
2. Start the server once to generate `plugins/MiniAuth/config.yml` and `messages.yml`.
3. Choose `database.type: sqlite` or `database.type: mysql` in `config.yml` and restart.

SQLite creates its database file in the plugin data directory. For MySQL, create the schema first; the plugin creates the `accounts` table.

## Commands

- `/register <password> <confirm>` (`/reg`)
- `/login <password>` (`/l`)
- `/logout`

This is a basic authentication plugin, not a replacement for a full-featured authentication/security suite. Test it on a staging server before production use. Java 25 runtime support depends also on the particular Paper server build.
