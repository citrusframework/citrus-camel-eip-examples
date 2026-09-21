# Project Standards

This rule file contains build tools, commands, and code style constraints for the project. Commands read this file to determine how to build, test, and format code.

- **Build tool:** Jekyll (site) + Maven (examples)
- **Build command:** `bundle exec jekyll build` (site), `cd examples/<name>/quarkus && mvn verify` or `cd examples/<name>/spring-boot && mvn verify` (examples)
- **Test command:** `cd examples/<name>/quarkus && mvn verify` or `cd examples/<name>/spring-boot && mvn verify`
- **Format command:** _(none)_
- **Module-specific build:** yes (always run `mvn` in the specific example's runtime directory)
- **Parallelized Maven:** no (resource intensive, do NOT parallelize Maven jobs)
- **Code style restrictions:**
  - Do NOT use Lombok
  - Java DSL for route definitions on both Quarkus and Spring Boot
  - Shipping domain for all examples (orders, inventory, payments, shipping, notifications)
