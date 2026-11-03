use std::borrow::Cow;
use std::path::PathBuf;
use std::str::FromStr;

use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use sqlx_core::error::Error;
use url::Url;

use super::{Target, TursoConnectOptions};

impl FromStr for TursoConnectOptions {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self, Error> {
        let url: Url = url.parse().map_err(Error::config)?;
        Self::from_db_url(&url)
    }
}

impl TursoConnectOptions {
    pub(crate) fn from_db_url(url: &Url) -> Result<Self, Error> {
        let scheme = url.scheme();
        let has_host = url.host_str().is_some_and(|h| !h.is_empty());

        let mut options = match scheme {
            "http" | "https" => remote_options(url, false)?,
            "turso" | "libsql" if has_host => remote_options(url, scheme == "libsql")?,
            "turso" | "libsql" => {
                let path = percent_decode_str(url.path())
                    .decode_utf8()
                    .map_err(Error::config)?;
                if path.is_empty() || path == ":memory:" {
                    Self::new().in_memory()
                } else {
                    Self::new().filename(PathBuf::from(&*path))
                }
            }
            other => {
                return Err(Error::Configuration(
                    format!("unsupported URL scheme for Turso: {other:?}").into(),
                ));
            }
        };

        for (key, value) in url.query_pairs() {
            match &*key {
                "authToken" => {
                    if !options.is_remote() {
                        return Err(Error::Configuration(
                            "`authToken` is only valid for remote URLs; use \
                             TursoConnectOptions::auth_token() with sync"
                                .into(),
                        ));
                    }
                    options = options.auth_token(value.into_owned());
                }
                "mode" if !options.is_remote() => match &*value {
                    "ro" => options = options.read_only(true),
                    "rw" => {}
                    "rwc" => options = options.create_if_missing(true),
                    "memory" => options = options.in_memory(),
                    _ => {
                        return Err(Error::Configuration(
                            format!("unknown value {value:?} for `mode`").into(),
                        ));
                    }
                },
                _ => {
                    return Err(Error::Configuration(
                        format!("unknown query parameter `{key}` in Turso connection URL").into(),
                    ));
                }
            }
        }

        Ok(options)
    }

    pub(crate) fn build_url(&self) -> Url {
        let mut url = match &self.target {
            Target::Memory => Url::parse("turso::memory:").expect("valid URL"),
            Target::File(path) => {
                let path = path.to_string_lossy();
                let encoded: Cow<'_, str> = utf8_percent_encode(&path, PATH_SET).into();
                Url::parse(&format!("turso:{encoded}")).expect("valid URL")
            }
            Target::Remote { url, .. } => Url::parse(url).expect("remote URL was validated"),
        };

        if !self.is_remote() {
            if self.read_only {
                url.query_pairs_mut().append_pair("mode", "ro");
            } else if self.create_if_missing {
                url.query_pairs_mut().append_pair("mode", "rwc");
            }
        }

        url
    }
}

/// Characters kept as-is when encoding a file path into a URL.
const PATH_SET: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'.')
    .remove(b'-')
    .remove(b'_')
    .remove(b'~')
    .remove(b':');

fn remote_options(url: &Url, libsql: bool) -> Result<TursoConnectOptions, Error> {
    let mut base = url.clone();
    base.set_query(None);
    base.set_fragment(None);
    let mut options = TursoConnectOptions::new().remote(base.as_str().trim_end_matches('/'));
    if let Target::Remote { libsql: l, .. } = &mut options.target {
        *l = libsql;
    }
    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(url: &str) -> Target {
        url.parse::<TursoConnectOptions>().unwrap().target
    }

    #[test]
    fn parses_local_urls() {
        assert_eq!(target("turso::memory:"), Target::Memory);
        assert_eq!(target("turso:"), Target::Memory);
        assert_eq!(target("turso:data.db"), Target::File("data.db".into()));
        assert_eq!(
            target("turso:///tmp/a%20b.db"),
            Target::File("/tmp/a b.db".into())
        );
        assert_eq!(target("libsql:data.db"), Target::File("data.db".into()));

        let opts: TursoConnectOptions = "turso:data.db?mode=rwc".parse().unwrap();
        assert!(opts.create_if_missing);
        let opts: TursoConnectOptions = "turso:data.db?mode=ro".parse().unwrap();
        assert!(opts.read_only);
    }

    #[test]
    fn parses_remote_urls() {
        assert_eq!(
            target("libsql://db-org.turso.io?authToken=abc"),
            Target::Remote {
                url: "libsql://db-org.turso.io".into(),
                libsql: true
            }
        );
        assert_eq!(
            target("turso://db-org.turso.io"),
            Target::Remote {
                url: "turso://db-org.turso.io".into(),
                libsql: false
            }
        );
        assert_eq!(
            target("https://db-org.turso.io/"),
            Target::Remote {
                url: "https://db-org.turso.io".into(),
                libsql: false
            }
        );

        let opts: TursoConnectOptions = "libsql://db.turso.io?authToken=abc".parse().unwrap();
        assert_eq!(opts.default_begin_sql(), "BEGIN");
        assert!(format!("{opts:?}").contains("<redacted>"));
        assert!(!format!("{opts:?}").contains("abc"));
    }

    #[test]
    fn rejects_bad_urls() {
        assert!("postgres://x".parse::<TursoConnectOptions>().is_err());
        assert!(
            "turso:data.db?authToken=x"
                .parse::<TursoConnectOptions>()
                .is_err()
        );
        assert!(
            "turso:data.db?bogus=1"
                .parse::<TursoConnectOptions>()
                .is_err()
        );
    }

    #[test]
    fn round_trips_to_url() {
        for url in [
            "turso::memory:",
            "turso:data.db?mode=rwc",
            "libsql://db.turso.io",
        ] {
            let opts: TursoConnectOptions = url.parse().unwrap();
            assert_eq!(opts.build_url().as_str(), url);
        }
    }
}
