//! SQL text split into statements: DataFusion's, and Timelake's own about rollups
//! (`CREATE`, `DROP` and `REFRESH MATERIALIZED ROLLUP`, spec 27).

use datafusion::common::TableReference;
use datafusion::sql::parser::DFParserBuilder;
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

use crate::error::{Error, Result};

/// One SQL statement, as [`Session::parse`](super::Session::parse) splits a script.
#[derive(Clone, Debug)]
pub enum Statement {
    /// A statement DataFusion plans: queries, `SET`, `SHOW`, `EXPLAIN` and the rest.
    Sql(Box<datafusion::sql::parser::Statement>),
    /// A statement about a rollup.
    Rollup(RollupStatement),
}

/// `WITH` options: key and value.
pub(crate) type Options = Vec<(String, String)>;

/// `CREATE`, `DROP` or `REFRESH MATERIALIZED ROLLUP`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RollupStatement {
    /// `CREATE MATERIALIZED ROLLUP [IF NOT EXISTS] <name> AS <query> [WITH (<key> = <value>, …)]`
    Create {
        /// The rollup table's name.
        name: TableReference,
        /// The `SELECT … GROUP BY …` that defines it.
        query: String,
        /// The `WITH` options: settings of the rollup table.
        options: Options,
        /// Do nothing if a table of that name exists.
        if_not_exists: bool,
    },
    /// `DROP MATERIALIZED ROLLUP [IF EXISTS] <name>`
    Drop {
        /// The rollup table's name.
        name: TableReference,
        /// Do nothing if it doesn't exist.
        if_exists: bool,
    },
    /// `REFRESH MATERIALIZED ROLLUP <name>`: bring it up to date now.
    Refresh {
        /// The rollup table's name.
        name: TableReference,
    },
}

/// The one item of `items`, if there is exactly one.
pub(super) fn only<T>(items: impl IntoIterator<Item = T>) -> Option<T> {
    let mut items = items.into_iter();
    let first = items.next()?;
    items.next().is_none().then_some(first)
}

/// Splits `sql` into statements at semicolons outside literals and comments.
pub(super) fn parse(sql: &str, dialect: &dyn Dialect, recursion_limit: usize) -> Result<Vec<Statement>> {
    let tokens = Tokenizer::new(dialect, sql).tokenize_with_location().map_err(|e| Error::invalid(e.to_string()))?;
    let offsets = LineOffsets::new(sql);
    let mut out = vec![];
    for piece in tokens.split(|t| t.token == Token::SemiColon) {
        let words: Vec<&TokenWithSpan> = piece.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
        let (Some(first), Some(last)) = (words.first(), words.last()) else { continue };
        if is_rollup_statement(&words) {
            out.push(Statement::Rollup(rollup_statement(sql, &offsets, &words)?));
            continue;
        }
        let (text, duckdb_asof) = without_asof(&sql[offsets.at(first.span.start)..offsets.at(last.span.end)], dialect)?;
        let text = text.as_str();
        let statements = DFParserBuilder::new(text)
            .with_dialect(dialect)
            .with_recursion_limit(recursion_limit)
            .build()?
            .parse_statements()?;
        let Some(mut s) = only(statements) else {
            return Err(Error::invalid(format!("can't parse {text:?} as one statement")));
        };
        super::asof::rewrite_statement(&mut s, &duckdb_asof).map_err(Error::invalid)?;
        out.push(Statement::Sql(Box::new(s)));
    }
    Ok(out)
}

/// `text` with the `ASOF` of each of DuckDB's `ASOF [LEFT] JOIN r ON …` blanked out (which
/// DataFusion's parser can't read), and where each such join's right side starts, with whether it
/// is a `LEFT` join. Snowflake's `ASOF JOIN … MATCH_CONDITION (…)` is left as it is.
fn without_asof(text: &str, dialect: &dyn Dialect) -> Result<(String, Vec<(Location, bool)>)> {
    let tokens = Tokenizer::new(dialect, text).tokenize_with_location().map_err(|e| Error::invalid(e.to_string()))?;
    let words: Vec<&TokenWithSpan> = tokens.iter().filter(|t| !matches!(t.token, Token::Whitespace(_))).collect();
    let offsets = LineOffsets::new(text);
    let mut blanked = text.to_owned();
    let mut found = vec![];
    for (i, w) in words.iter().enumerate() {
        if !is_word(w, "ASOF") {
            continue;
        }
        let rest = &words[i + 1..];
        let (left, join) = match rest {
            [l, o, j, ..] if is_word(l, "LEFT") && is_word(o, "OUTER") && is_word(j, "JOIN") => (true, 3),
            [l, j, ..] if is_word(l, "LEFT") && is_word(j, "JOIN") => (true, 2),
            [j, ..] if is_word(j, "JOIN") => (false, 1),
            _ => continue,
        };
        // Snowflake's form names its match condition before the next ASOF.
        let next = rest.iter().position(|t| is_word(t, "ASOF")).unwrap_or(rest.len());
        if rest[..next].iter().any(|t| is_word(t, "MATCH_CONDITION")) {
            continue;
        }
        let Some(right) = rest.get(join) else { continue };
        let (start, end) = (offsets.at(w.span.start), offsets.at(w.span.end));
        blanked.replace_range(start..end, &" ".repeat(end - start));
        found.push((right.span.start, left));
    }
    Ok((blanked, found))
}

/// Byte offsets of the tokenizer's line and column locations.
struct LineOffsets<'a> {
    sql: &'a str,
    starts: Vec<usize>,
}

impl<'a> LineOffsets<'a> {
    fn new(sql: &'a str) -> Self {
        let starts = std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect();
        Self { sql, starts }
    }

    /// The byte offset of a location (line and column count from 1; columns in characters).
    fn at(&self, l: Location) -> usize {
        let line = usize::try_from(l.line).unwrap_or(usize::MAX).saturating_sub(1);
        let Some(&start) = self.starts.get(line) else { return self.sql.len() };
        let column = usize::try_from(l.column).unwrap_or(usize::MAX).saturating_sub(1);
        self.sql[start..].char_indices().nth(column).map_or(self.sql.len(), |(i, _)| start + i)
    }
}

fn is_word(t: &TokenWithSpan, word: &str) -> bool {
    matches!(&t.token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

fn is_rollup_statement(words: &[&TokenWithSpan]) -> bool {
    words.len() >= 3
        && ["CREATE", "DROP", "REFRESH"].iter().any(|w| is_word(words[0], w))
        && is_word(words[1], "MATERIALIZED")
        && is_word(words[2], "ROLLUP")
}

/// A cursor over a statement's tokens, without whitespace.
struct Words<'a, 'b> {
    words: &'b [&'a TokenWithSpan],
    at: usize,
}

impl<'a> Words<'a, '_> {
    fn peek(&self) -> Option<&'a TokenWithSpan> {
        self.words.get(self.at).copied()
    }

    fn next(&mut self) -> Option<&'a TokenWithSpan> {
        let t = self.peek();
        self.at += 1;
        t
    }

    /// Consumes `words` if they come next.
    fn keywords(&mut self, words: &[&str]) -> bool {
        let matched = words.iter().enumerate().all(|(i, w)| self.words.get(self.at + i).is_some_and(|t| is_word(t, w)));
        if matched {
            self.at += words.len();
        }
        matched
    }

    /// A table name: `table`, `namespace.table` or `catalog.namespace.table`, each part a plain or
    /// quoted identifier.
    fn table_name(&mut self) -> Result<TableReference> {
        let mut parts = vec![];
        loop {
            match self.next().map(|t| &t.token) {
                Some(Token::Word(w)) => parts.push(w.value.clone()),
                other => return Err(Error::invalid(format!("expected a table name, found {other:?}"))),
            }
            if self.peek().map(|t| &t.token) != Some(&Token::Period) {
                break;
            }
            self.at += 1;
        }
        Ok(match parts.as_slice() {
            [table] => TableReference::bare(table.as_str()),
            [namespace, table] => TableReference::partial(namespace.as_str(), table.as_str()),
            [catalog, namespace, table] => TableReference::full(catalog.as_str(), namespace.as_str(), table.as_str()),
            _ => return Err(Error::invalid(format!("{} isn't a table name", parts.join(".")))),
        })
    }

    fn end(&self, statement: &str) -> Result<()> {
        match self.peek() {
            None => Ok(()),
            Some(t) => Err(Error::invalid(format!("unexpected {} after {statement}", t.token))),
        }
    }
}

fn rollup_statement(sql: &str, offsets: &LineOffsets<'_>, words: &[&TokenWithSpan]) -> Result<RollupStatement> {
    let mut w = Words { words, at: 3 };
    if is_word(words[0], "DROP") {
        let if_exists = w.keywords(&["IF", "EXISTS"]);
        let name = w.table_name()?;
        w.end("DROP MATERIALIZED ROLLUP <name>")?;
        return Ok(RollupStatement::Drop { name, if_exists });
    }
    if is_word(words[0], "REFRESH") {
        let name = w.table_name()?;
        w.end("REFRESH MATERIALIZED ROLLUP <name>")?;
        return Ok(RollupStatement::Refresh { name });
    }
    let if_not_exists = w.keywords(&["IF", "NOT", "EXISTS"]);
    let name = w.table_name()?;
    if !w.keywords(&["AS"]) {
        return Err(Error::invalid("expected AS <query> after CREATE MATERIALIZED ROLLUP <name>"));
    }
    let query_words = &words[w.at..];
    let (query_words, options) = match with_options(query_words)? {
        Some((at, options)) => (&query_words[..at], options),
        None => (query_words, vec![]),
    };
    let (Some(first), Some(last)) = (query_words.first(), query_words.last()) else {
        return Err(Error::invalid("CREATE MATERIALIZED ROLLUP needs a query after AS"));
    };
    let query = sql[offsets.at(first.span.start)..offsets.at(last.span.end)].to_string();
    Ok(RollupStatement::Create { name, query, options, if_not_exists })
}

/// A trailing `WITH (key = value, …)`: where it starts, and the options.
fn with_options(words: &[&TokenWithSpan]) -> Result<Option<(usize, Options)>> {
    if words.last().map(|t| &t.token) != Some(&Token::RParen) {
        return Ok(None);
    }
    let mut depth = 0;
    let mut open = None;
    for (i, t) in words.iter().enumerate().rev() {
        match t.token {
            Token::RParen => depth += 1,
            Token::LParen => {
                depth -= 1;
                if depth == 0 {
                    open = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(open) = open.filter(|&i| i > 0 && is_word(words[i - 1], "WITH")) else { return Ok(None) };
    let mut options = vec![];
    let inner = &words[open + 1..words.len() - 1];
    for option in inner.split(|t| t.token == Token::Comma) {
        let [key, eq, value] = option else {
            return Err(Error::invalid("WITH options are key = value, separated by commas"));
        };
        let key = match &key.token {
            Token::Word(w) => w.value.clone(),
            Token::SingleQuotedString(s) => s.clone(),
            other => return Err(Error::invalid(format!("expected an option name, found {other}"))),
        };
        if eq.token != Token::Eq {
            return Err(Error::invalid(format!("expected = after {key}")));
        }
        let value = match &value.token {
            Token::SingleQuotedString(s) => s.clone(),
            Token::Number(n, _) => n.clone(),
            Token::Word(w) => w.value.clone(),
            other => return Err(Error::invalid(format!("expected a value for {key}, found {other}"))),
        };
        options.push((key, value));
    }
    Ok(Some((open - 1, options)))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::GenericDialect;

    use super::*;

    fn parse(sql: &str) -> Vec<Statement> {
        super::parse(sql, &GenericDialect {}, 50).unwrap()
    }

    #[test]
    fn scripts_split_outside_literals_and_comments() {
        let statements = parse("SELECT 'a;b'; -- c;d\nSELECT 2;;\n  SELECT 'é' ;");
        assert_eq!(statements.len(), 3);
        assert!(statements.iter().all(|s| matches!(s, Statement::Sql(_))));
    }

    #[test]
    fn rollup_statements() {
        let sql = "select 1;\ncreate materialized rollup if not exists iot.r_1m as\n  SELECT device, time_bucket(INTERVAL '1 \
                   minute', time) AS b, avg(value) FROM iot.r GROUP BY 1, 2\nWITH (retention = '2y', \"refresh_interval\" \
                   = '5m', min_snapshots = 3);\nDROP MATERIALIZED ROLLUP IF EXISTS r_1m; refresh materialized rollup \
                   \"iot\".\"r_1m\"";
        let statements = parse(sql);
        let rollups: Vec<&RollupStatement> = statements
            .iter()
            .filter_map(|s| match s {
                Statement::Rollup(r) => Some(r),
                Statement::Sql(_) => None,
            })
            .collect();
        assert_eq!(
            rollups,
            [
                &RollupStatement::Create {
                    name: TableReference::partial("iot", "r_1m"),
                    query: "SELECT device, time_bucket(INTERVAL '1 minute', time) AS b, avg(value) FROM iot.r GROUP BY 1, 2"
                        .into(),
                    options: vec![
                        ("retention".into(), "2y".into()),
                        ("refresh_interval".into(), "5m".into()),
                        ("min_snapshots".into(), "3".into()),
                    ],
                    if_not_exists: true,
                },
                &RollupStatement::Drop { name: TableReference::bare("r_1m"), if_exists: true },
                &RollupStatement::Refresh { name: TableReference::partial("iot", "r_1m") },
            ]
        );
        // Names are parsed once: a quoted part keeps its dots.
        let refresh = parse("REFRESH MATERIALIZED ROLLUP timelake.\"i.ot\".r_1m");
        let expected = RollupStatement::Refresh { name: TableReference::full("timelake", "i.ot", "r_1m") };
        assert!(matches!(&refresh[0], Statement::Rollup(r) if *r == expected));
        // A query ending in a parenthesis isn't taken for options.
        let create =
            parse("CREATE MATERIALIZED ROLLUP r AS SELECT max(v) FROM t GROUP BY date_bin(INTERVAL '1 h', ts)");
        assert!(matches!(&create[0], Statement::Rollup(RollupStatement::Create { options, .. }) if options.is_empty()));
        for bad in
            ["CREATE MATERIALIZED ROLLUP r", "DROP MATERIALIZED ROLLUP r extra", "CREATE MATERIALIZED ROLLUP r AS"]
        {
            assert!(super::parse(bad, &GenericDialect {}, 50).is_err(), "{bad}");
        }
    }
}
