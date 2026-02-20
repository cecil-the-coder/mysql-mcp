/// Parse a MySQL CLI-style connection string.
/// Supports: -hHOST or -h HOST, -PPORT or -P PORT, -uUSER or -u USER, -pPASS or -p PASS
/// Also supports long forms: --host=HOST, --port=PORT, --user=USER, --password=PASS
/// Positional argument at the end is the database name.
pub fn parse_mysql_cli_string(s: &str) -> ParsedCliConnection {
    let mut result = ParsedCliConnection::default();
    let tokens: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;

    while i < tokens.len() {
        let tok = tokens[i];

        // Long forms: --host=VALUE, --port=VALUE, --user=VALUE, --password=VALUE, --database=VALUE
        if let Some(rest) = tok.strip_prefix("--") {
            if let Some(val) = rest.strip_prefix("host=") {
                result.host = Some(val.to_string());
            } else if let Some(val) = rest.strip_prefix("port=") {
                result.port = val.parse().ok();
            } else if let Some(val) = rest.strip_prefix("user=") {
                result.user = Some(val.to_string());
            } else if let Some(val) = rest.strip_prefix("password=") {
                result.password = Some(val.to_string());
            } else if let Some(val) = rest.strip_prefix("database=") {
                result.database = Some(val.to_string());
            } else if rest == "host" {
                if let Some(next) = tokens.get(i + 1) {
                    result.host = Some(next.to_string());
                    i += 1;
                }
            } else if rest == "port" {
                if let Some(next) = tokens.get(i + 1) {
                    result.port = next.parse().ok();
                    i += 1;
                }
            } else if rest == "user" {
                if let Some(next) = tokens.get(i + 1) {
                    result.user = Some(next.to_string());
                    i += 1;
                }
            } else if rest == "password" {
                if let Some(next) = tokens.get(i + 1) {
                    result.password = Some(next.to_string());
                    i += 1;
                }
            } else if rest == "database" {
                if let Some(next) = tokens.get(i + 1) {
                    result.database = Some(next.to_string());
                    i += 1;
                }
            }
            i += 1;
            continue;
        }

        // Short forms starting with a single dash
        if let Some(rest) = tok.strip_prefix('-') {
            if rest.is_empty() {
                i += 1;
                continue;
            }

            let flag = &rest[..1];
            let inline_val = if rest.len() > 1 { Some(&rest[1..]) } else { None };

            match flag {
                "h" => {
                    if let Some(val) = inline_val {
                        result.host = Some(val.to_string());
                    } else if let Some(next) = tokens.get(i + 1) {
                        if !next.starts_with('-') {
                            result.host = Some(next.to_string());
                            i += 1;
                        }
                    }
                }
                "P" => {
                    if let Some(val) = inline_val {
                        result.port = val.parse().ok();
                    } else if let Some(next) = tokens.get(i + 1) {
                        if !next.starts_with('-') {
                            result.port = next.parse().ok();
                            i += 1;
                        }
                    }
                }
                "u" => {
                    if let Some(val) = inline_val {
                        result.user = Some(val.to_string());
                    } else if let Some(next) = tokens.get(i + 1) {
                        if !next.starts_with('-') {
                            result.user = Some(next.to_string());
                            i += 1;
                        }
                    }
                }
                "p" => {
                    // -p can have inline value or separate (but conventionally inline)
                    if let Some(val) = inline_val {
                        result.password = Some(val.to_string());
                    } else if let Some(next) = tokens.get(i + 1) {
                        if !next.starts_with('-') {
                            result.password = Some(next.to_string());
                            i += 1;
                        }
                    }
                }
                _ => {}
            }

            i += 1;
            continue;
        }

        // Positional argument: database name (last non-flag token)
        result.database = Some(tok.to_string());
        i += 1;
    }

    result
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ParsedCliConnection {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_inline() {
        let p = parse_mysql_cli_string("-hlocalhost -P3306 -uroot -psecret mydb");
        assert_eq!(p.host, Some("localhost".to_string()));
        assert_eq!(p.port, Some(3306));
        assert_eq!(p.user, Some("root".to_string()));
        assert_eq!(p.password, Some("secret".to_string()));
        assert_eq!(p.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_short_spaced() {
        let p = parse_mysql_cli_string("-h localhost -P 3306 -u root -p secret mydb");
        assert_eq!(p.host, Some("localhost".to_string()));
        assert_eq!(p.port, Some(3306));
        assert_eq!(p.user, Some("root".to_string()));
        assert_eq!(p.password, Some("secret".to_string()));
        assert_eq!(p.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_long_forms() {
        let p = parse_mysql_cli_string("--host=db.example.com --port=3307 --user=admin --password=pass123 --database=appdb");
        assert_eq!(p.host, Some("db.example.com".to_string()));
        assert_eq!(p.port, Some(3307));
        assert_eq!(p.user, Some("admin".to_string()));
        assert_eq!(p.password, Some("pass123".to_string()));
        assert_eq!(p.database, Some("appdb".to_string()));
    }

    #[test]
    fn test_partial() {
        let p = parse_mysql_cli_string("-hlocalhost mydb");
        assert_eq!(p.host, Some("localhost".to_string()));
        assert_eq!(p.database, Some("mydb".to_string()));
        assert_eq!(p.port, None);
    }

    #[test]
    fn test_empty() {
        let p = parse_mysql_cli_string("");
        assert_eq!(p, ParsedCliConnection::default());
    }

    #[test]
    fn test_short_flags_inline() {
        let result = parse_mysql_cli_string("-hlocalhost -P3306 -uroot -psecret mydb");
        assert_eq!(result.host, Some("localhost".to_string()));
        assert_eq!(result.port, Some(3306));
        assert_eq!(result.user, Some("root".to_string()));
        assert_eq!(result.password, Some("secret".to_string()));
        assert_eq!(result.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_short_flags_spaced() {
        let result = parse_mysql_cli_string("-h localhost -P 3306 -u root -p secret mydb");
        assert_eq!(result.host, Some("localhost".to_string()));
        assert_eq!(result.port, Some(3306));
        assert_eq!(result.user, Some("root".to_string()));
        assert_eq!(result.password, Some("secret".to_string()));
    }

    #[test]
    fn test_long_flags() {
        let result = parse_mysql_cli_string("--host=myhost --port=1234 --user=admin --password=p@ss");
        assert_eq!(result.host, Some("myhost".to_string()));
        assert_eq!(result.port, Some(1234));
        assert_eq!(result.user, Some("admin".to_string()));
        assert_eq!(result.password, Some("p@ss".to_string()));
    }

    #[test]
    fn test_long_flags_spaced() {
        let result = parse_mysql_cli_string("--host myhost --port 1234 --user admin --password pass");
        assert_eq!(result.host, Some("myhost".to_string()));
        assert_eq!(result.port, Some(1234));
        assert_eq!(result.user, Some("admin".to_string()));
        assert_eq!(result.password, Some("pass".to_string()));
    }

    #[test]
    fn test_missing_optional_fields() {
        let result = parse_mysql_cli_string("-hlocalhost");
        assert_eq!(result.host, Some("localhost".to_string()));
        assert_eq!(result.port, None);
        assert_eq!(result.user, None);
        assert_eq!(result.database, None);
    }

    #[test]
    fn test_password_with_special_chars() {
        // Inline -p with special chars (no spaces, so it's one token)
        let result = parse_mysql_cli_string("-hlocalhost -pmy@special#pass!");
        assert_eq!(result.password, Some("my@special#pass!".to_string()));
    }

    #[test]
    fn test_empty_string() {
        let result = parse_mysql_cli_string("");
        assert_eq!(result.host, None);
        assert_eq!(result.port, None);
    }

    #[test]
    fn test_database_via_long_flag() {
        let result = parse_mysql_cli_string("--host=localhost --database=myapp");
        assert_eq!(result.host, Some("localhost".to_string()));
        assert_eq!(result.database, Some("myapp".to_string()));
    }

    #[test]
    fn test_positional_database() {
        let result = parse_mysql_cli_string("-hlocalhost testdb");
        assert_eq!(result.host, Some("localhost".to_string()));
        assert_eq!(result.database, Some("testdb".to_string()));
    }

    #[test]
    fn test_only_database_positional() {
        let result = parse_mysql_cli_string("mydb");
        assert_eq!(result.database, Some("mydb".to_string()));
        assert_eq!(result.host, None);
        assert_eq!(result.port, None);
    }
}
