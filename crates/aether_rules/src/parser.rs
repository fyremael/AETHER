use aether_ast::{
    AggregateFunction, AggregateTerm, Atom, AttributeId, ExplainSpec, ExplainTarget,
    ExtensionalFact, Literal, NamedExplainSpec, NamedQuerySpec, PolicyEnvelope, PredicateId,
    PredicateRef, QueryAst, QuerySpec, RuleAst, RuleId, RuleProgram, TemporalView, Term, Value,
    Variable,
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
    pub query: Option<QuerySpec>,
    pub queries: Vec<NamedQuerySpec>,
    pub explains: Vec<NamedExplainSpec>,
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
    let schema_section = single_section(&sections, "schema")?;
    let predicates_section = single_section(&sections, "predicates")?;
    let rules_section = single_section(&sections, "rules")?;
    let facts_section = optional_single_section(&sections, "facts")?;
    let materialize_section = optional_single_section(&sections, "materialize")?;
    let query_sections = sections.get("query").map(Vec::as_slice).unwrap_or(&[]);
    let explain_sections = sections.get("explain").map(Vec::as_slice).unwrap_or(&[]);

    let mut schema = parse_schema_section(schema_section)?;
    let predicate_refs = parse_predicates_section(predicates_section, &mut schema)?;
    let facts = parse_facts_section(facts_section, &predicate_refs)?;
    let rules = parse_rules_section(rules_section, &predicate_refs)?;
    let materialized = parse_materialize_section(materialize_section, &predicate_refs)?;
    let (query, queries) = parse_query_sections(query_sections, &predicate_refs)?;
    let explains = parse_explain_sections(explain_sections, &predicate_refs)?;

    Ok(DslDocument {
        program: RuleProgram {
            predicates: predicate_refs.values().cloned().collect(),
            rules,
            materialized,
            facts,
        },
        schema,
        query,
        queries,
        explains,
    })
}

#[derive(Clone, Debug)]
struct Section {
    name: String,
    argument: Option<String>,
    line: usize,
    entries: Vec<(usize, String)>,
}

fn collect_sections(input: &str) -> Result<IndexMap<String, Vec<Section>>, ParseError> {
    let mut sections: IndexMap<String, Vec<Section>> = IndexMap::new();
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
                if !is_repeatable_section(&section.name) && sections.contains_key(&section.name) {
                    return Err(ParseError::DuplicateSection {
                        line: section.line,
                        section: section.name,
                    });
                }
                sections
                    .entry(section.name.clone())
                    .or_default()
                    .push(section);
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
        "schema" | "predicates" | "rules" | "materialize" | "facts" | "query" | "explain" => {
            name.to_owned()
        }
        "queries" => "query".to_owned(),
        "explains" => "explain".to_owned(),
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

fn is_repeatable_section(name: &str) -> bool {
    matches!(name, "query" | "explain")
}

fn single_section<'a>(
    sections: &'a IndexMap<String, Vec<Section>>,
    name: &'static str,
) -> Result<&'a Section, ParseError> {
    let entries = sections.get(name).ok_or(ParseError::MissingSection(name))?;
    entries.first().ok_or(ParseError::MissingSection(name))
}

fn optional_single_section<'a>(
    sections: &'a IndexMap<String, Vec<Section>>,
    name: &'static str,
) -> Result<Option<&'a Section>, ParseError> {
    Ok(sections.get(name).and_then(|entries| entries.first()))
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
        let head = parse_atom(*line, head.trim(), predicate_refs, true)?;
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

fn parse_facts_section(
    section: Option<&Section>,
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<Vec<ExtensionalFact>, ParseError> {
    let Some(section) = section else {
        return Ok(Vec::new());
    };

    let mut facts = Vec::new();
    for (line, entry) in &section.entries {
        if entry.is_empty() {
            continue;
        }

        let (call, annotation_text) = split_call_and_suffix(*line, entry)?;
        let (name, args) = parse_call(*line, call)?;
        let predicate =
            predicate_refs
                .get(name)
                .ok_or_else(|| ParseError::UnknownPredicateName {
                    line: *line,
                    name: name.into(),
                })?;
        if predicate.arity != args.len() {
            return Err(ParseError::PredicateArityMismatch {
                line: *line,
                predicate: predicate.name.clone(),
                expected: predicate.arity,
                actual: args.len(),
            });
        }

        let values = args
            .iter()
            .map(|token| parse_fact_value(*line, token))
            .collect::<Result<Vec<_>, _>>()?;
        let policy = parse_policy_annotations(*line, annotation_text)?;

        facts.push(ExtensionalFact {
            predicate: predicate.clone(),
            values,
            policy,
            provenance: None,
        });
    }

    Ok(facts)
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

fn parse_query_sections(
    sections: &[Section],
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<(Option<QuerySpec>, Vec<NamedQuerySpec>), ParseError> {
    let mut named_queries = Vec::new();
    let mut seen_names = IndexSet::new();
    let mut primary_query = None;

    for section in sections {
        let query = parse_single_query_section(section, predicate_refs)?;
        if !seen_names.insert(section.argument.clone()) {
            return Err(ParseError::DuplicateNamedSection {
                line: section.line,
                section: "query".into(),
                name: section.argument.clone(),
            });
        }
        if primary_query.is_none() || section.argument.is_none() {
            primary_query = Some(query.clone());
        }
        named_queries.push(NamedQuerySpec {
            name: section.argument.clone(),
            spec: query,
        });
    }

    Ok((primary_query, named_queries))
}

fn parse_single_query_section(
    section: &Section,
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<QuerySpec, ParseError> {
    let mut view = TemporalView::Current;
    let mut goals = Vec::new();
    let mut keep = Vec::new();

    for (line, entry) in &section.entries {
        if let Some(rest) = entry.strip_prefix("as_of ") {
            let Some(element) = rest.trim().strip_prefix('e') else {
                return Err(ParseError::InvalidQueryEntry {
                    line: *line,
                    entry: entry.clone(),
                });
            };
            let element = element
                .parse::<u64>()
                .map_err(|_| ParseError::InvalidQueryEntry {
                    line: *line,
                    entry: entry.clone(),
                })?;
            view = TemporalView::AsOf(aether_ast::ElementId::new(element));
            continue;
        }
        if entry == "current" {
            view = TemporalView::Current;
            continue;
        }
        if let Some(rest) = entry
            .strip_prefix("goal ")
            .or_else(|| entry.strip_prefix("find "))
        {
            goals.push(parse_atom(*line, rest.trim(), predicate_refs, false)?);
            continue;
        }
        if let Some(rest) = entry.strip_prefix("keep ") {
            keep.extend(
                split_top_level(rest.trim(), ',', *line)?
                    .into_iter()
                    .filter(|name| !name.is_empty())
                    .map(Variable::new),
            );
            continue;
        }

        return Err(ParseError::InvalidQueryEntry {
            line: *line,
            entry: entry.clone(),
        });
    }

    Ok(QuerySpec {
        view,
        query: QueryAst { goals, keep },
    })
}

fn parse_explain_sections(
    sections: &[Section],
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<Vec<NamedExplainSpec>, ParseError> {
    let mut explains = Vec::new();
    let mut seen_names = IndexSet::new();

    for section in sections {
        if !seen_names.insert(section.argument.clone()) {
            return Err(ParseError::DuplicateNamedSection {
                line: section.line,
                section: "explain".into(),
                name: section.argument.clone(),
            });
        }
        explains.push(NamedExplainSpec {
            name: section.argument.clone(),
            spec: parse_single_explain_section(section, predicate_refs)?,
        });
    }

    Ok(explains)
}

fn parse_single_explain_section(
    section: &Section,
    predicate_refs: &IndexMap<String, PredicateRef>,
) -> Result<ExplainSpec, ParseError> {
    let mut view = TemporalView::Current;
    let mut target = None;

    for (line, entry) in &section.entries {
        if let Some(rest) = entry.strip_prefix("as_of ") {
            let Some(element) = rest.trim().strip_prefix('e') else {
                return Err(ParseError::InvalidExplainEntry {
                    line: *line,
                    entry: entry.clone(),
                });
            };
            let element = element
                .parse::<u64>()
                .map_err(|_| ParseError::InvalidExplainEntry {
                    line: *line,
                    entry: entry.clone(),
                })?;
            view = TemporalView::AsOf(aether_ast::ElementId::new(element));
            continue;
        }
        if entry == "current" {
            view = TemporalView::Current;
            continue;
        }
        if entry == "plan" {
            if target.replace(ExplainTarget::Plan).is_some() {
                return Err(ParseError::InvalidExplainEntry {
                    line: *line,
                    entry: entry.clone(),
                });
            }
            continue;
        }
        if let Some(rest) = entry.strip_prefix("tuple ") {
            let atom = parse_atom(*line, rest.trim(), predicate_refs, false)?;
            if !atom.terms.iter().all(|term| matches!(term, Term::Value(_))) {
                return Err(ParseError::NonGroundExplainTuple {
                    line: *line,
                    entry: entry.clone(),
                });
            }
            if target.replace(ExplainTarget::Tuple(atom)).is_some() {
                return Err(ParseError::InvalidExplainEntry {
                    line: *line,
                    entry: entry.clone(),
                });
            }
            continue;
        }

        return Err(ParseError::InvalidExplainEntry {
            line: *line,
            entry: entry.clone(),
        });
    }

    let target = target.ok_or(ParseError::MissingExplainTarget {
        line: section.line,
        name: section.argument.clone(),
    })?;
    Ok(ExplainSpec { view, target })
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
            false,
        )?));
    }
    if let Some(atom) = literal.strip_prefix('!') {
        return Ok(Literal::Negative(parse_atom(
            line,
            atom.trim(),
            predicate_refs,
            false,
        )?));
    }

    Ok(Literal::Positive(parse_atom(
        line,
        literal.trim(),
        predicate_refs,
        false,
    )?))
}

fn parse_atom(
    line: usize,
    atom: &str,
    predicate_refs: &IndexMap<String, PredicateRef>,
    allow_aggregates: bool,
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
        .map(|token| parse_term(line, token, allow_aggregates))
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

fn parse_term(line: usize, token: &str, allow_aggregates: bool) -> Result<Term, ParseError> {
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

    if let Some(inner) = token
        .strip_prefix("entity(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let value = inner
            .trim()
            .parse::<u64>()
            .map_err(|_| ParseError::InvalidTerm {
                line,
                text: token.into(),
            })?;
        return Ok(Term::Value(Value::Entity(aether_ast::EntityId::new(value))));
    }

    if token.contains('(') || token.contains(')') {
        if allow_aggregates {
            if let Some(aggregate) = parse_aggregate_term(line, token)? {
                return Ok(Term::Aggregate(aggregate));
            }
        }
        return Err(ParseError::InvalidTerm {
            line,
            text: token.into(),
        });
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

fn parse_fact_value(line: usize, token: &str) -> Result<Value, ParseError> {
    let token = token.trim();
    match parse_term(line, token, false)? {
        Term::Value(value) => Ok(value),
        Term::Variable(_) | Term::Aggregate(_) => Err(ParseError::InvalidFactValue {
            line,
            text: token.into(),
        }),
    }
}

fn parse_aggregate_term(line: usize, token: &str) -> Result<Option<AggregateTerm>, ParseError> {
    let Ok((name, args)) = parse_call(line, token) else {
        return Ok(None);
    };

    let function = match name {
        "count" => AggregateFunction::Count,
        "sum" => AggregateFunction::Sum,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        _ => return Ok(None),
    };

    if args.len() != 1 {
        return Err(ParseError::InvalidTerm {
            line,
            text: token.into(),
        });
    }

    let variable = args[0].trim();
    if variable.is_empty()
        || variable.starts_with('"')
        || variable.contains('(')
        || variable.contains(')')
    {
        return Err(ParseError::InvalidTerm {
            line,
            text: token.into(),
        });
    }

    Ok(Some(AggregateTerm {
        function,
        variable: Variable::new(variable),
    }))
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

fn split_call_and_suffix(line: usize, entry: &str) -> Result<(&str, &str), ParseError> {
    let mut in_string = false;
    let mut paren_depth = 0usize;

    for (index, ch) in entry.char_indices() {
        match ch {
            '"' => in_string = !in_string,
            '(' if !in_string => paren_depth += 1,
            ')' if !in_string => {
                if paren_depth == 0 {
                    return Err(ParseError::UnbalancedDelimiter { line });
                }
                paren_depth -= 1;
                if paren_depth == 0 {
                    return Ok((&entry[..=index], entry[index + 1..].trim()));
                }
            }
            _ => {}
        }
    }

    Err(ParseError::InvalidCall {
        line,
        text: entry.into(),
    })
}

fn parse_policy_annotations(line: usize, rest: &str) -> Result<Option<PolicyEnvelope>, ParseError> {
    if rest.is_empty() {
        return Ok(None);
    }

    let mut capability = None;
    let mut visibility = None;
    let mut remaining = rest.trim();

    while !remaining.is_empty() {
        let Some(annotation) = remaining.strip_prefix('@') else {
            return Err(ParseError::InvalidPolicyAnnotation {
                line,
                text: rest.into(),
            });
        };
        let (call, suffix) = split_call_and_suffix(line, annotation)?;
        let (name, args) = parse_call(line, call)?;
        if args.len() != 1 {
            return Err(ParseError::InvalidPolicyAnnotation {
                line,
                text: call.into(),
            });
        }
        let value = parse_fact_value(line, &args[0])?;
        let Value::String(value) = value else {
            return Err(ParseError::InvalidPolicyAnnotation {
                line,
                text: call.into(),
            });
        };

        match name {
            "capability" => capability = Some(value),
            "visibility" => visibility = Some(value),
            _ => {
                return Err(ParseError::InvalidPolicyAnnotation {
                    line,
                    text: call.into(),
                })
            }
        }

        remaining = suffix.trim();
    }

    Ok(Some(PolicyEnvelope {
        capability,
        visibility,
    }))
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
    #[error("line {line}: invalid fact value {text}")]
    InvalidFactValue { line: usize, text: String },
    #[error("line {line}: invalid query entry {entry}")]
    InvalidQueryEntry { line: usize, entry: String },
    #[error("line {line}: invalid explain entry {entry}")]
    InvalidExplainEntry { line: usize, entry: String },
    #[error("line {line}: invalid policy annotation {text}")]
    InvalidPolicyAnnotation { line: usize, text: String },
    #[error("line {line}: duplicate materialized predicate {name}")]
    DuplicateMaterializedPredicate { line: usize, name: String },
    #[error("line {line}: duplicate {section} section name {name:?}")]
    DuplicateNamedSection {
        line: usize,
        section: String,
        name: Option<String>,
    },
    #[error("line {line}: explain section {name:?} does not declare a target")]
    MissingExplainTarget { line: usize, name: Option<String> },
    #[error("line {line}: explain tuple must be ground: {entry}")]
    NonGroundExplainTuple { line: usize, entry: String },
    #[error("line {line}: unbalanced delimiter in DSL input")]
    UnbalancedDelimiter { line: usize },
    #[error("line {line}: schema error: {source}")]
    Schema { line: usize, source: SchemaError },
}

#[cfg(test)]
mod tests {
    use super::{DefaultDslParser, DslParser, ParseError};
    use crate::{DefaultRuleCompiler, RuleCompiler};
    use aether_ast::{
        AggregateFunction, Atom, ExplainTarget, Literal, NamedQuerySpec, PredicateId, PredicateRef,
        QueryAst, QuerySpec, TemporalView, Term, Value,
    };
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
        assert!(document.query.is_none());
        assert!(document.queries.is_empty());
        assert!(document.explains.is_empty());

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
    fn parses_facts_queries_as_of_and_policy_annotations() {
        let document = DefaultDslParser
            .parse_document(
                r#"
                schema v2 {
                  attr task.status: ScalarLWW<String>
                }

                predicates {
                  execution_attempt(Entity, String, U64)
                  task_ready(Entity)
                }

                facts {
                  execution_attempt(entity(1), "worker-a", 1) @capability("executor") @visibility("ops")
                }

                rules {
                  task_ready(x) <- task_ready(x)
                }

                query {
                  as_of e5
                  goal task_ready(x)
                  keep x
                }
                "#,
            )
            .expect("parse document with facts and query");

        assert_eq!(document.schema.version, "v2");
        assert_eq!(document.program.facts.len(), 1);
        assert_eq!(
            document.program.facts[0].policy,
            Some(aether_ast::PolicyEnvelope {
                capability: Some("executor".into()),
                visibility: Some("ops".into()),
            })
        );
        assert_eq!(
            document.query,
            Some(QuerySpec {
                view: TemporalView::AsOf(aether_ast::ElementId::new(5)),
                query: QueryAst {
                    goals: vec![Atom {
                        predicate: PredicateRef {
                            id: PredicateId::new(2),
                            name: "task_ready".into(),
                            arity: 1,
                        },
                        terms: vec![Term::Variable(aether_ast::Variable::new("x"))],
                    }],
                    keep: vec![aether_ast::Variable::new("x")],
                },
            })
        );
        assert_eq!(
            document.queries,
            vec![NamedQuerySpec {
                name: None,
                spec: document.query.clone().expect("primary query"),
            }]
        );
        assert!(document.explains.is_empty());
    }

    #[test]
    fn parses_head_aggregates_for_bounded_aggregation_rules() {
        let document = DefaultDslParser
            .parse_document(
                r#"
                schema {
                  attr task.depends_on: RefSet<Entity>
                }

                predicates {
                  task_depends_on(Entity, Entity)
                  dependency_count(Entity, U64)
                }

                rules {
                  dependency_count(task, count(dep)) <- task_depends_on(task, dep)
                }

                materialize {
                  dependency_count
                }
                "#,
            )
            .expect("parse aggregate rule");

        let aggregate_rule = &document.program.rules[0];
        assert!(matches!(
            &aggregate_rule.head.terms[1],
            Term::Aggregate(aggregate)
                if aggregate.function == AggregateFunction::Count
                    && aggregate.variable == aether_ast::Variable::new("dep")
        ));
    }

    #[test]
    fn parses_named_queries_and_explain_directives() {
        let document = DefaultDslParser
            .parse_document(
                r#"
                schema {
                  attr task.depends_on: RefSet<Entity>
                }

                predicates {
                  task_depends_on(Entity, Entity)
                  depends_transitive(Entity, Entity)
                }

                rules {
                  depends_transitive(x, y) <- task_depends_on(x, y)
                }

                materialize {
                  depends_transitive
                }

                query ready_now {
                  current
                  find depends_transitive(entity(1), y)
                  keep y
                }

                query ready_then {
                  as_of e7
                  goal depends_transitive(entity(1), y)
                  keep y
                }

                explain proof_now {
                  current
                  tuple depends_transitive(entity(1), entity(2))
                }

                explain plan_view {
                  plan
                }
                "#,
            )
            .expect("parse named queries and explain directives");

        assert_eq!(document.queries.len(), 2);
        assert_eq!(document.queries[0].name.as_deref(), Some("ready_now"));
        assert_eq!(document.queries[1].name.as_deref(), Some("ready_then"));
        assert_eq!(document.query, Some(document.queries[0].spec.clone()));

        assert_eq!(document.explains.len(), 2);
        assert!(matches!(
            &document.explains[0].spec.target,
            ExplainTarget::Tuple(atom)
                if atom.terms
                    == vec![
                        Term::Value(Value::Entity(aether_ast::EntityId::new(1))),
                        Term::Value(Value::Entity(aether_ast::EntityId::new(2))),
                    ]
        ));
        assert_eq!(document.explains[1].name.as_deref(), Some("plan_view"));
        assert_eq!(document.explains[1].spec.target, ExplainTarget::Plan);
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
