# Security policy

## Supported versions

Every `@shadow-js/*` package and `create-shadow-app` share one version number. Security fixes are released for the latest minor of the
current major version and for the previous minor for six months after the next minor ships.

| Version | Supported |
| --- | --- |
| 1.x (latest minor) | yes |
| 1.x (previous minor, six months) | security and regression fixes |
| earlier than 1.0.0 | no |

## Reporting a vulnerability

Please do not open a public issue for a security problem. Use GitHub's private vulnerability reporting: open the **Security** tab of
this repository and choose **Report a vulnerability**. Include:

- the package and version (`npm ls @shadow-js/realtime`),
- a description of the problem and its impact,
- a minimal reproduction (a repository, a test or exact steps),
- whether the problem is already public or has a deadline.

You will get an acknowledgment within three working days and a first assessment within ten. We keep you informed while a fix is prepared,
agree on a disclosure date with you (normally within 90 days of the report), and credit you in the advisory unless you prefer to stay anonymous.

## Scope

In scope: the code published from this repository. Out of scope: vulnerabilities in third-party services or in dependencies with no ShadowJS-specific
exposure (report those upstream), findings that need a malicious local developer or an already compromised build machine, and denial of service
by traffic volumes that any server would reject by configuration. Releases are published with npm provenance.
