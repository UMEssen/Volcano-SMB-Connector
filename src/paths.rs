//! Path handling.
//!
//! Two forms of a path are used:
//! - the **SMB path**: relative to the share root, `/`-separated, original case —
//!   what `smb2` accepts for `stat`/`list_directory`;
//! - the **key**: the SMB path normalised for use as Kafka key and state key
//!   (separators unified, no leading/trailing `/`, optionally lowercased).

/// Normalise any path spelling into a share-relative, `/`-separated path.
///
/// Accepts `\\server\share\a\b`, `/a//b/`, `a\b` and returns `a/b`.
/// A UNC prefix (`\\server\share`) is stripped.
pub fn normalize(path: &str) -> String {
    let unified = path.replace('\\', "/");
    let rest = match unified.strip_prefix("//") {
        // UNC: drop server and share components.
        Some(unc) => unc.splitn(3, '/').nth(2).unwrap_or("").to_string(),
        None => unified,
    };
    rest.split('/')
        .filter(|c| !c.is_empty() && *c != ".")
        .collect::<Vec<_>>()
        .join("/")
}

/// Kafka/state key for a share-relative path.
pub fn key(path: &str, case_insensitive: bool) -> String {
    let n = normalize(path);
    if case_insensitive {
        n.to_lowercase()
    } else {
        n
    }
}

/// Join a share-relative directory and a child name/path.
pub fn join(dir: &str, child: &str) -> String {
    let dir = normalize(dir);
    let child = normalize(child);
    match (dir.is_empty(), child.is_empty()) {
        (true, _) => child,
        (_, true) => dir,
        _ => format!("{dir}/{child}"),
    }
}

/// Final component of a path.
pub fn file_name(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_variants() {
        assert_eq!(
            normalize(r"\\server\share\Unter\Datei.pdf"),
            "Unter/Datei.pdf"
        );
        assert_eq!(normalize(r"\\server\share"), "");
        assert_eq!(normalize("/a//b/./c/"), "a/b/c");
        assert_eq!(normalize(r"a\b"), "a/b");
        assert_eq!(normalize(""), "");
    }

    #[test]
    fn keys() {
        assert_eq!(key(r"Unter\Datei.PDF", false), "Unter/Datei.PDF");
        assert_eq!(key(r"Unter\Datei.PDF", true), "unter/datei.pdf");
        assert_eq!(key("Ärzte/Übersicht.pdf", true), "ärzte/übersicht.pdf");
    }

    #[test]
    fn join_and_name() {
        assert_eq!(join("", "a.txt"), "a.txt");
        assert_eq!(join("in/", "/sub/a.txt"), "in/sub/a.txt");
        assert_eq!(join("in", ""), "in");
        assert_eq!(file_name("in/sub/a.txt"), "a.txt");
        assert_eq!(file_name("a.txt"), "a.txt");
    }
}
