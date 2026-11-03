//! Splitting query strings into statements and working out bind parameters.
//!
//! sqlx-sqlite gets both from `sqlite3_prepare_v3()` (the unparsed tail) and
//! `sqlite3_bind_parameter_count()` / `sqlite3_bind_parameter_name()`. The Turso
//! client exposes neither, so the query is tokenized here with Turso's own parser,
//! applying the same parameter numbering rules the engine uses.

use std::collections::HashMap;
use std::sync::Arc;

use sqlx_core::error::Error;
use turso_parser::lexer::Lexer;
use turso_parser::parser::Parser;
use turso_parser::token::TokenType;

/// How the argument for one parameter slot is chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Parameter {
    /// `?`, or a slot no variable refers to: takes the next argument in sequence.
    Anonymous,
    /// `?NNN` or `$NNN`: argument `NNN` (1-based).
    Numbered(usize),
    /// `:name`, `@name`, `$name`: not supported for binding (same as sqlx-sqlite).
    Named(String),
}

/// One statement of a (possibly multi-statement) query string.
#[derive(Debug)]
pub(crate) struct ParsedStatement {
    pub(crate) sql: String,
    /// Parameter slots by index; element `i` is the parameter bound at index `i + 1`.
    pub(crate) parameters: Vec<Parameter>,
}

/// A query string split into statements.
#[derive(Debug)]
pub(crate) struct ParsedQuery {
    pub(crate) statements: Vec<ParsedStatement>,
}

impl ParsedQuery {
    pub(crate) fn parse(sql: &str) -> Result<Arc<Self>, Error> {
        let bytes = sql.as_bytes();
        let mut parser = Parser::new(bytes);
        let mut statements = Vec::new();
        let mut start = 0;

        loop {
            match parser.next_cmd() {
                Ok(Some(_)) => {
                    let end = parser.offset();
                    let text = sql[start..end]
                        .trim_start_matches(|c: char| c == ';' || c.is_whitespace())
                        .trim();
                    let text = text.strip_suffix(';').unwrap_or(text).trim_end();
                    if !text.is_empty() {
                        statements.push(ParsedStatement {
                            sql: text.to_string(),
                            parameters: parameters(text)?,
                        });
                    }
                    start = end;
                }
                Ok(None) => break,
                Err(error) => {
                    // Let the engine report the syntax error with its own message,
                    // for exactly the statement that fails.
                    let text = sql[start..].trim();
                    statements.push(ParsedStatement {
                        sql: text.to_string(),
                        parameters: parameters(text).unwrap_or_default(),
                    });
                    tracing::trace!(%error, "query failed to parse; deferring to the engine");
                    break;
                }
            }
        }

        Ok(Arc::new(Self { statements }))
    }
}

/// Assign parameter indexes the way SQLite (and Turso) do: `?` takes the largest
/// index so far plus one, `?NNN` takes `NNN`, and a named parameter takes the next
/// index on first use and reuses it afterwards.
fn parameters(sql: &str) -> Result<Vec<Parameter>, Error> {
    let mut slots: Vec<Option<Parameter>> = Vec::new();
    let mut named: HashMap<&[u8], usize> = HashMap::new();
    let mut last = 0usize;

    let assign = |slots: &mut Vec<Option<Parameter>>, index: usize, parameter: Parameter| {
        if slots.len() < index {
            slots.resize(index, None);
        }
        slots[index - 1].get_or_insert(parameter);
    };

    for token in Lexer::new(sql.as_bytes()) {
        let Ok(token) = token else {
            // Tokenizer errors surface from the engine when the statement is prepared.
            break;
        };
        if token.token_type != TokenType::TK_VARIABLE {
            continue;
        }

        let value = token.value;
        match value {
            b"?" => {
                last += 1;
                assign(&mut slots, last, Parameter::Anonymous);
            }
            [b'?', digits @ ..] => {
                let n = parse_index(digits).ok_or_else(|| {
                    sqlx_core::err_protocol!(
                        "invalid positional parameter: {}",
                        String::from_utf8_lossy(value)
                    )
                })?;
                last = last.max(n);
                assign(&mut slots, n, Parameter::Numbered(n));
            }
            _ => {
                let index = *named.entry(value).or_insert_with(|| {
                    last += 1;
                    last
                });
                let parameter = match value {
                    [b'$', digits @ ..] if parse_index(digits).is_some() => {
                        Parameter::Numbered(parse_index(digits).expect("checked above"))
                    }
                    _ => Parameter::Named(String::from_utf8_lossy(value).into_owned()),
                };
                assign(&mut slots, index, parameter);
            }
        }
    }

    Ok(slots
        .into_iter()
        .map(|slot| slot.unwrap_or(Parameter::Anonymous))
        .collect())
}

fn parse_index(digits: &[u8]) -> Option<usize> {
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(digits)
        .ok()?
        .parse()
        .ok()
        .filter(|n| *n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(sql: &str) -> Vec<String> {
        ParsedQuery::parse(sql)
            .unwrap()
            .statements
            .iter()
            .map(|s| s.sql.clone())
            .collect()
    }

    #[test]
    fn splits_statements() {
        assert_eq!(split("SELECT 1"), ["SELECT 1"]);
        assert_eq!(split("SELECT 1; SELECT 2;"), ["SELECT 1", "SELECT 2"]);
        assert_eq!(split("  ;; SELECT ';' ; "), ["SELECT ';'"]);
        assert_eq!(
            split("CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; SELECT 2; END; SELECT 3"),
            [
                "CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; SELECT 2; END",
                "SELECT 3"
            ]
        );
        assert!(split("").is_empty());
        assert!(split("-- just a comment").is_empty());
    }

    #[test]
    fn numbers_parameters() {
        use Parameter::*;
        let p = |sql| parameters(sql).unwrap();

        assert_eq!(p("SELECT ?, ?"), [Anonymous, Anonymous]);
        assert_eq!(p("SELECT ?2, ?1"), [Numbered(1), Numbered(2)]);
        assert_eq!(p("SELECT ?3"), [Anonymous, Anonymous, Numbered(3)]);
        assert_eq!(p("SELECT ?2, ?"), [Anonymous, Numbered(2), Anonymous]);
        assert_eq!(p("SELECT $1, $2, $1"), [Numbered(1), Numbered(2)]);
        assert_eq!(
            p("SELECT :a, :b, :a"),
            [Named(":a".into()), Named(":b".into())]
        );
        assert_eq!(p("SELECT '?' , \"?\""), []);
    }
}
