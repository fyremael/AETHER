use aether_ast::{
    Atom, AttributeId, Literal, PredicateId, PredicateRef, RuleAst, RuleId, RuleProgram, Term,
    Value, Variable,
};
use aether_schema::{
    AttributeClass, AttributeSchema, PredicateSignature, Schema, SchemaError, ValueType,
};
use indexmap::{IndexMap, IndexSet};
use thiserror::Error;

pub trait DslParser {
    fn parse_document(&self, input: &str) -> Result<DslDocument, ParseError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct DslDocument {
    pub schema: Schema,
    pub program: RuleProgram,
}

#[derive(Default)]
pub struct DefaultDslParser;

impl DslParser for DefaultDslParser {
    fn parse_document(&self, input: &str) -> Result<DslDocument, ParseError> {
        parse_document(input)
    }
}

fn parse_document(input: &str) -> Result<DslDocument, ParseError> {
    let sections = collect_sections(input)?;
    let schema_section = sections
        .get("schema")
        .ok_or(ParseError::MissingSection("schema"))?;
    let predicates_section = sections
        .get("predicates")
        .ok_or(ParseError::MissingSection("predicates"))?;
    let rules_section = sections
        .get("rules")
        .ok_or(ParseError::MissingSection("rules"))?;
    let materialize_section = sections.get("materialize");

    let mut schema = parse_schema_section(schema_section)?;
    let predicate_refs = parse_predicates_section(predicates_section, &mut schema)?;
    let rules = parse_rules_section(rules_section, &predicate_refs)?;
    let materialized = parse_materialize_section(materialize_section, &predicate_refs)?;

    Ok(DslDocument {
        program: RuleProgram {
            predicates: predicate_refs.values().cloned().collect(),
            rules,
            materialized,
        },
        schema,
    })
}

#[derive(Clone, Debug)]
struct Section {
    name: String,
    argument: Option<String>,
    line: usize,
    entries: Vec<(usize, String)>,
}

fn collect_sections(input: &str) -> Result<IndexMap<String, Section>, ParseError> {
    let mut sections = IndexMap::new();
    let mut current: Option<Section> = None;

    for (index, raw_line) in input.lines().enumerate() {
        let line_number = index + 1;
        let line = strip_comments(raw_line).trim().to_owned();
        if line.is_empty() {
            continue;
        }

        if let Some(section) = current.as_mut() {
            if line == "}" {
                let section = current.take().expect("section present");
                if sections.contains_key(&section.name) {
                    return Err(ParseError::DuplicateSection {
                        line: section.line,
                        section: section.name,
                    });
                }
                sections.insert(section.name.clone(), section);
                continue;
            }

            section
                .entries
                .push((line_number, trim_statement(&line).to_owned()));
            continue;
        }

        let Some(header) = line.strip_suffix('{') else {
            return Err(ParseError::UnexpectedTopLevel {
                line: line_number,
                content: line,
            });
        };
        let header = header.trim();
        let (name, argument) = parse_section_header(line_number, header)?;
        current = Some(Section {
            name,
            argument,
            line: line_number,
            entries: Vec::new(),
        });
    }

    if let Some(section) = current {
        return Err(ParseError::UnterminatedSection {
            line: section.line,
            section: section.name,
        });
    }

    Ok(sections)
}

fn parse_section_header(line: usize, header: &str) -> Result<(String, Option<String>), ParseError> {
    let mut parts = header.split_whitespace();
    let name = parts
        .next()
        .ok_or_else(|| ParseError::InvalidSectionHeader {
            line,
            header: header.into(),
        })?;
    let name = match name {
        "schema" | "predicates" | "rules" | "materialize" => name.to_owned(),
        "materialized" => "materialize".to_owned(),
        other => {
            return Err(ParseError::UnknownSection {
                line,
                section: other.into(),
            })
        }
    };
    let argument = parts.next().map(ToOwned::to_owned);
    if parts.next().is_some() {
        return Err(ParseError::InvalidSectionHeader {
            line,
            header: header.into(),
        });
    }

    Ok((name, argument))
}

fn parse_schema_section(section: &Section) -> Result<Schema, ParseError> {
    let mut schema = Schema::new(section.argument.as_deref().unwrap_or("v1"));
    let mut next_attribute_id = 1u64;

    for (line, entry) in &section.entries {
        if entry.is_empty() {
            continue;
        }

        let Some(entry) = entry.strip_prefix("attr ") else {
            return Err(ParseError::InvalidSchemaEntry {
                line: *line,
                entry: entry.clone(),
            });
        };
        let Some((name, spec)) = entry.split_once(':') else {
            return Err(ParseError::InvalidSchemaEntry {
                line: *line,
                entry: entry.into(),
            });
        };
        let name = name.trim();
        let spec = spec.trim();
        let (class, value_type) = parse_attribute_spec(*line, spec)?;
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(next_attribute_id),
                name: name.into(),
                class,
                value_type,
            })
            .map_err(|source| ParseError::Schema {
                line: *line,
                source,
            })?;
        next_attribute_id += 1;
    }

    Ok(schema)
}

fn parse_predicates_section(
    section: &Section,
    schema: &mut Schema,
) -> Result<IndexMap<String, PredicateRef>, ParseError> {
    let mut predicate_refs = IndexMap::new();
    let mut next_predicate_id = 1u64;

    for (line, entry) in &section.entries {
        if entry.is_empty() {
            continue;
        }

        let (name, args) = parse_call(*line, entry)?;
        let fields = args
            .iter()
            .map(|token| parse_value_type(*line, token))
            .collect::<Result<Vec<_>, _>>()?;
        let id = PredicateId::new(next_predicate_id);
        schema
            .register_predicate(PredicateSignature {
                id,
                name: name.into(),
                fields: fields.clone(),
            })
            .map_err(|source| ParseError::Schema {
                line: *line,
                source,
            })?;
        predicate_refs.insert(
            name.into(),
            PredicateRef {
                id,
                name: name.into(),
                arity: fields.len(),
            },
        );
        next_predicate_id += 1;
    }

    Ok(predicate_refs)
}

fn parse_rules_section(
    section: &Section,
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<Vec<RuleAst>, ParseError> {
    let mut rules = Vec::new();

    for (index, (line, entry)) in section.entries.iter().enumerate() {
        if entry.is_empty() {
            continue;
        }

        let Some((head, body)) = entry.split_once("<-") else {
            return Err(ParseError::InvalidRule {
                line: *line,
                rule: entry.clone(),
            });
        };
        let head = parse_atom(*line, head.trim(), predicate_refs)?;
        let body = split_top_level(body.trim(), ',', *line)?
            .into_iter()
            .filter(|literal| !literal.is_empty())
            .map(|literal| parse_literal(*line, &literal, predicate_refs))
            .collect::<Result<Vec<_>, _>>()?;

        rules.push(RuleAst {
            id: RuleId::new(index as u64 + 1),
            head,
            body,
        });
    }

    Ok(rules)
}

fn parse_materialize_section(
    section: Option<&Section>,
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<Vec<PredicateId>, ParseError> {
    let Some(section) = section else {
        return Ok(Vec::new());
    };

    let mut seen = IndexSet::new();
    let mut materialized = Vec::new();

    for (line, entry) in &section.entries {
        for name in split_top_level(entry, ',', *line)? {
            if name.is_empty() {
                continue;
            }
            let predicate = predicate_refs.get(name.as_str()).ok_or_else(|| {
                ParseError::UnknownPredicateName {
                    line: *line,
                    name: name.clone(),
                }
            })?;
            if !seen.insert(predicate.id) {
                return Err(ParseError::DuplicateMaterializedPredicate { line: *line, name });
            }
            materialized.push(predicate.id);
        }
    }

    Ok(materialized)
}

fn parse_attribute_spec(
    line: usize,
    spec: &str,
) -> Result<(AttributeClass, ValueType), ParseError> {
    let Some(open) = spec.find('<') else {
        return Err(ParseError::InvalidSchemaEntry {
            line,
            entry: spec.into(),
        });
    };
    let Some(close) = spec.rfind('>') else {
        return Err(ParseError::InvalidSchemaEntry {
            line,
            entry: spec.into(),
        });
    };
    if close + 1 != spec.len() {
        return Err(ParseError::InvalidSchemaEntry {
            line,
            entry: spec.into(),
        });
    }

    let class = match spec[..open].trim() {
        "ScalarLww" | "ScalarLWW" => AttributeClass::ScalarLww,
        "SetAddWins" => AttributeClass::SetAddWins,
        "SequenceRga" | "SequenceRGA" => AttributeClass::SequenceRga,
        "RefScalar" => AttributeClass::RefScalar,
        "RefSet" => AttributeClass::RefSet,
        _ => {
            return Err(ParseError::InvalidSchemaEntry {
                line,
                entry: spec.into(),
            })
        }
    };

    let value_type = parse_value_type(line, &spec[open + 1..close])?;
    Ok((class, value_type))
}

fn parse_literal(
    line: usize,
    literal: &str,
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<Literal, ParseError> {
    if let Some(atom) = literal.strip_prefix("not ") {
        return Ok(Literal::Negative(parse_atom(
            line,
            atom.trim(),
            predicate_refs,
        )?));
    }
    if let Some(atom) = literal.strip_prefix('!') {
        return Ok(Literal::Negative(parse_atom(
            line,
            atom.trim(),
            predicate_refs,
        )?));
    }

    Ok(Literal::Positive(parse_atom(
        line,
        literal.trim(),
        predicate_refs,
    )?))
}

fn parse_atom(
    line: usize,
    atom: &str,
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<Atom, ParseError> {
    let (name, args) = parse_call(line, atom)?;
    let predicate = predicate_refs
        .get(name)
        .ok_or_else(|| ParseError::UnknownPredicateName {
            line,
            name: name.into(),
        })?;
    if predicate.arity != args.len() {
        return Err(ParseError::PredicateArityMismatch {
            line,
            predicate: predicate.name.clone(),
            expected: predicate.arity,
            actual: args.len(),
        });
    }

    let terms = args
        .iter()
        .map(|token| parse_term(line, token))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Atom {
        predicate: predicate.clone(),
        terms,
    })
}

fn parse_call(line: usize, text: &str) -> Result<(&str, Vec<String>), ParseError> {
    let Some(open) = text.find('(') else {
        return Err(ParseError::InvalidCall {
            line,
            text: text.into(),
        });
    };
    let Some(close) = text.rfind(')') else {
        return Err(ParseError::InvalidCall {
            line,
            text: text.into(),
        });
    };
    if close + 1 != text.len() {
        return Err(ParseError::InvalidCall {
            line,
            text: text.into(),
        });
    }

    let name = text[..open].trim();
    if name.is_empty() {
        return Err(ParseError::InvalidCall {
            line,
            text: text.into(),
        });
    }

    let inner = text[open + 1..close].trim();
    let args = if inner.is_empty() {
        Vec::new()
    } else {
        split_top_level(inner, ',', line)?
    };

    Ok((name, args))
}

fn parse_term(line: usize, token: &str) -> Result<Term, ParseError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(ParseError::InvalidTerm {
            line,
            text: token.into(),
        });
    }

    if token.starts_with('"') {
        return Ok(Term::Value(Value::String(parse_string_literal(
            line, token,
        )?)));
    }

    match token {
        "true" => return Ok(Term::Value(Value::Bool(true))),
        "false" => return Ok(Term::Value(Value::Bool(false))),
        "null" => return Ok(Term::Value(Value::Null)),
        _ => {}
    }

    if let Ok(value) = token.parse::<i64>() {
        if token.starts_with('-') {
            return Ok(Term::Value(Value::I64(value)));
        }
    }
    if let Ok(value) = token.parse::<u64>() {
        return Ok(Term::Value(Value::U64(value)));
    }
    if token.contains('.') {
        if let Ok(value) = token.parse::<f64>() {
            return Ok(Term::Value(Value::F64(value)));
        }
    }

    Ok(Term::Variable(Variable::new(token)))
}

fn parse_string_literal(line: usize, token: &str) -> Result<String, ParseError> {
    if token.len() < 2 || !token.ends_with('"') {
        return Err(ParseError::InvalidTerm {
            line,
            text: token.into(),
        });
    }

    let mut result = String::new();
    let mut chars = token[1..token.len() - 1].chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let Some(escaped) = chars.next() else {
                return Err(ParseError::InvalidTerm {
                    line,
                    text: token.into(),
                });
            };
            match escaped {
                '\\' => result.push('\\'),
                '"' => result.push('"'),
                'n' => result.push('\n'),
                'r' => result.push('\r'),
                't' => result.push('\t'),
                _ => {
                    return Err(ParseError::InvalidTerm {
                        line,
                        text: token.into(),
                    })
                }
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

fn parse_value_type(line: usize, token: &str) -> Result<ValueType, ParseError> {
    let token = token.trim();
    if let Some(inner) = token
        .strip_prefix("List<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        return Ok(ValueType::List(Box::new(parse_value_type(line, inner)?)));
    }

    match token {
        "Bool" => Ok(ValueType::Bool),
        "I64" => Ok(ValueType::I64),
        "U64" => Ok(ValueType::U64),
        "F64" => Ok(ValueType::F64),
        "String" => Ok(ValueType::String),
        "Bytes" => Ok(ValueType::Bytes),
        "Entity" => Ok(ValueType::Entity),
        _ => Err(ParseError::InvalidType {
            line,
            text: token.into(),
        }),
    }
}

fn split_top_level(input: &str, separator: char, line: usize) -> Result<Vec<String>, ParseError> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0usize;
    let mut angle_depth = 0usize;
    let mut in_string = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                in_string = !in_string;
                current.push(ch);
            }
            '\\' if in_string => {
                current.push(ch);
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            '(' if !in_string => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' if !in_string => {
                if paren_depth == 0 {
                    return Err(ParseError::UnbalancedDelimiter { line });
                }
                paren_depth -= 1;
                current.push(ch);
            }
            '<' if !in_string => {
                angle_depth += 1;
                current.push(ch);
            }
            '>' if !in_string => {
                if angle_depth == 0 {
                    return Err(ParseError::UnbalancedDelimiter { line });
                }
                angle_depth -= 1;
                current.push(ch);
            }
            _ if ch == separator && !in_string && paren_depth == 0 && angle_depth == 0 => {
                parts.push(current.trim().to_owned());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if in_string || paren_depth != 0 || angle_depth != 0 {
        return Err(ParseError::UnbalancedDelimiter { line });
    }

    parts.push(current.trim().to_owned());
    Ok(parts)
}

fn strip_comments(line: &str) -> &str {
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in line.char_indices() {
        match ch {
            '"' if !escaped => in_string = !in_string,
            '#' if !in_string => return &line[..index],
            '\\' if in_string => {
                escaped = !escaped;
                continue;
            }
            _ => {}
        }
        escaped = false;
    }

    line
}

fn trim_statement(line: &str) -> &str {
    line.trim_end_matches([';', '.'])
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("line {line}: unexpected top-level content: {content}")]
    UnexpectedTopLevel { line: usize, content: String },
    #[error("line {line}: invalid section header {header}")]
    InvalidSectionHeader { line: usize, header: String },
    #[error("line {line}: unknown section {section}")]
    UnknownSection { line: usize, section: String },
    #[error("line {line}: duplicate section {section}")]
    DuplicateSection { line: usize, section: String },
    #[error("section {0} is required")]
    MissingSection(&'static str),
    #[error("line {line}: section {section} is missing a closing brace")]
    UnterminatedSection { line: usize, section: String },
    #[error("line {line}: invalid schema entry {entry}")]
    InvalidSchemaEntry { line: usize, entry: String },
    #[error("line {line}: invalid predicate or atom call {text}")]
    InvalidCall { line: usize, text: String },
    #[error("line {line}: invalid rule {rule}")]
    InvalidRule { line: usize, rule: String },
    #[error("line {line}: unknown predicate {name}")]
    UnknownPredicateName { line: usize, name: String },
    #[error("line {line}: predicate {predicate} has arity {actual}, expected {expected}")]
    PredicateArityMismatch {
        line: usize,
        predicate: String,
        expected: usize,
        actual: usize,
    },
    #[error("line {line}: invalid type {text}")]
    InvalidType { line: usize, text: String },
    #[error("line {line}: invalid term {text}")]
    InvalidTerm { line: usize, text: String },
    #[error("line {line}: duplicate materialized predicate {name}")]
    DuplicateMaterializedPredicate { line: usize, name: String },
    #[error("line {line}: unbalanced delimiter in DSL input")]
    UnbalancedDelimiter { line: usize },
    #[error("line {line}: schema error: {source}")]
    Schema { line: usize, source: SchemaError },
}

#[cfg(test)]
mod tests {
    use super::{DefaultDslParser, DslParser, ParseError};
    use crate::{DefaultRuleCompiler, RuleCompiler};
    use aether_ast::{Literal, PredicateId, Term, Value};
    use aether_schema::{AttributeClass, ValueType};

    #[test]
    fn parses_document_and_compiles_recursive_program() {
        let document = DefaultDslParser
            .parse_document(
                r#"
                schema v1 {
                  attr task.depends_on: RefSet<Entity>
                  attr task.labels: SetAddWins<String>
                }

                predicates {
                  task_depends_on(Entity, Entity)
                  depends_transitive(Entity, Entity)
                }

                rules {
                  depends_transitive(x, y) <- task_depends_on(x, y)
                  depends_transitive(x, z) <- depends_transitive(x, y), task_depends_on(y, z)
                }

                materialize {
                  depends_transitive
                }
                "#,
            )
            .expect("parse dsl document");

        assert_eq!(document.schema.version, "v1");
        assert_eq!(
            document
                .schema
                .attribute(&aether_ast::AttributeId::new(1))
                .expect("first attribute")
                .class,
            AttributeClass::RefSet
        );
        assert_eq!(
            document
                .schema
                .predicate(&PredicateId::new(1))
                .expect("first predicate")
                .fields,
            vec![ValueType::Entity, ValueType::Entity]
        );
        assert_eq!(document.program.rules.len(), 2);
        assert_eq!(document.program.materialized, vec![PredicateId::new(2)]);

        let compiled = DefaultRuleCompiler
            .compile(&document.schema, &document.program)
            .expect("compile parsed program");
        assert_eq!(
            compiled.extensional_bindings.get(&PredicateId::new(1)),
            Some(&aether_ast::AttributeId::new(1))
        );
    }

    #[test]
    fn parses_negation_and_constant_terms() {
        let document = DefaultDslParser
            .parse_document(
                r#"
                schema {
                  attr task.status: ScalarLWW<String>
                }

                predicates {
                  task_status(Entity, String)
                  task(Entity)
                  blocked(Entity)
                }

                rules {
                  blocked(x) <- task(x), not task_status(x, "ready"), task_status(x, "blocked"), task_status(x, "retry-1")
                }

                materialize {
                  blocked
                }
                "#,
            )
            .expect("parse rule with negation and constants");

        let rule = &document.program.rules[0];
        assert!(matches!(rule.body[1], Literal::Negative(_)));
        let Literal::Positive(atom) = &rule.body[2] else {
            panic!("expected positive literal");
        };
        assert_eq!(
            atom.terms,
            vec![
                Term::Variable(aether_ast::Variable::new("x")),
                Term::Value(Value::String("blocked".into())),
            ]
        );
    }

    #[test]
    fn rejects_unknown_predicates() {
        let error = DefaultDslParser
            .parse_document(
                r#"
                schema {
                  attr task.status: ScalarLWW<String>
                }

                predicates {
                  task_status(Entity, String)
                }

                rules {
                  blocked(x) <- task_status(x, "ready")
                }
                "#,
            )
            .expect_err("unknown rule head predicate should fail");

        assert!(matches!(
            error,
            ParseError::UnknownPredicateName { name, .. } if name == "blocked"
        ));
    }

    #[test]
    fn rejects_unknown_types_and_duplicate_materialize_entries() {
        let unknown_type = DefaultDslParser
            .parse_document(
                r#"
                schema {
                  attr task.owner: RefScalar<Task>
                }

                predicates {
                  task_owner(Entity, Entity)
                }

                rules {
                  task_owner(x, y) <- task_owner(x, y)
                }
                "#,
            )
            .expect_err("unknown type alias should fail");
        assert!(matches!(unknown_type, ParseError::InvalidType { text, .. } if text == "Task"));

        let duplicate_materialize = DefaultDslParser
            .parse_document(
                r#"
                schema {
                  attr task.status: ScalarLWW<String>
                }

                predicates {
                  task_status(Entity, String)
                }

                rules {
                  task_status(x, s) <- task_status(x, s)
                }

                materialize {
                  task_status
                  task_status
                }
                "#,
            )
            .expect_err("duplicate materialize should fail");
        assert!(matches!(
            duplicate_materialize,
            ParseError::DuplicateMaterializedPredicate { name, .. } if name == "task_status"
        ));
    }
}
