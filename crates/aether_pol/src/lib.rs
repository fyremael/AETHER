use aether_ast::{ExtensionalFact, PolicyEnvelope, PredicateId, PredicateRef, SourceRef, Value};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Version string for the first AETHER-POL semantic vocabulary.
pub const AETHER_POL_VERSION: &str = "pol.v0.1";

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

string_id!(PolityId);
string_id!(CharterId);
string_id!(CommonsId);
string_id!(GuildId);
string_id!(AgentId);
string_id!(WorkObjectId);
string_id!(ClaimId);
string_id!(EvidenceId);
string_id!(CritiqueId);
string_id!(VerificationId);
string_id!(DecisionId);
string_id!(RouteProposalId);
string_id!(RouteDecisionId);
string_id!(RouterUpdateId);
string_id!(LedgerEntryId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkObjectKind {
    Task,
    ClaimReview,
    Incident,
    ResearchBrief,
    CodeChange,
    Benchmark,
    DecisionRecord,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkObjectState {
    Open,
    Routed,
    Claimed,
    InReview,
    Accepted,
    Retained,
    Rejected,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationVerdict {
    Supported,
    Refuted,
    Inconclusive,
    NeedsHumanReview,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CritiqueSeverity {
    Note,
    Concern,
    Blocking,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouterUpdateDisposition {
    Accepted,
    Retained,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Polity {
    pub id: PolityId,
    pub name: String,
    pub charter_id: CharterId,
    pub commons_id: CommonsId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Guild {
    pub id: GuildId,
    pub polity_id: PolityId,
    pub name: String,
    pub purpose: String,
    pub charter_id: CharterId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentContract {
    pub agent_id: AgentId,
    pub guild_id: GuildId,
    pub role: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub obligations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permissions: Vec<String>,
    pub trust_domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkObject {
    pub id: WorkObjectId,
    pub polity_id: PolityId,
    pub kind: WorkObjectKind,
    pub state: WorkObjectState,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_guild_id: Option<GuildId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    pub id: ClaimId,
    pub work_object_id: WorkObjectId,
    pub author_agent_id: AgentId,
    pub statement: String,
    pub confidence: f32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_ids: Vec<EvidenceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvidenceBundle {
    pub id: EvidenceId,
    pub claim_id: ClaimId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<SourceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Critique {
    pub id: CritiqueId,
    pub claim_id: ClaimId,
    pub critic_agent_id: AgentId,
    pub severity: CritiqueSeverity,
    pub objection: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Verification {
    pub id: VerificationId,
    pub claim_id: ClaimId,
    pub verifier_agent_id: AgentId,
    pub verdict: VerificationVerdict,
    pub confidence: f32,
    pub rationale: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub id: DecisionId,
    pub work_object_id: WorkObjectId,
    pub decided_by_agent_id: AgentId,
    pub outcome: String,
    pub rationale: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supporting_claim_ids: Vec<ClaimId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteProposal {
    pub id: RouteProposalId,
    pub work_object_id: WorkObjectId,
    pub router_agent_id: AgentId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_guild_ids: Vec<GuildId>,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteDecision {
    pub id: RouteDecisionId,
    pub proposal_id: RouteProposalId,
    pub selected_guild_id: GuildId,
    pub selected_by_agent_id: AgentId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RouterUpdate {
    pub id: RouterUpdateId,
    pub route_decision_id: RouteDecisionId,
    pub disposition: RouterUpdateDisposition,
    pub utility_delta: f32,
    pub regret_note: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyEnvelope>,
}

pub const POLITY_DECLARED_ID: PredicateId = PredicateId(20_001);
pub const GUILD_DECLARED_ID: PredicateId = PredicateId(20_002);
pub const AGENT_CONTRACTED_ID: PredicateId = PredicateId(20_003);
pub const WORK_OBJECT_DECLARED_ID: PredicateId = PredicateId(20_004);
pub const CLAIM_POSTED_ID: PredicateId = PredicateId(20_005);
pub const EVIDENCE_ATTACHED_ID: PredicateId = PredicateId(20_006);
pub const CRITIQUE_POSTED_ID: PredicateId = PredicateId(20_007);
pub const VERIFICATION_POSTED_ID: PredicateId = PredicateId(20_008);
pub const DECISION_POSTED_ID: PredicateId = PredicateId(20_009);
pub const ROUTE_PROPOSED_ID: PredicateId = PredicateId(20_010);
pub const ROUTE_DECIDED_ID: PredicateId = PredicateId(20_011);
pub const ROUTER_UPDATE_POSTED_ID: PredicateId = PredicateId(20_012);

pub fn predicate_catalog() -> Vec<PredicateRef> {
    vec![
        predicate(POLITY_DECLARED_ID, "polity_declared", 4),
        predicate(GUILD_DECLARED_ID, "guild_declared", 5),
        predicate(AGENT_CONTRACTED_ID, "agent_contracted", 7),
        predicate(WORK_OBJECT_DECLARED_ID, "work_object_declared", 6),
        predicate(CLAIM_POSTED_ID, "claim_posted", 6),
        predicate(EVIDENCE_ATTACHED_ID, "evidence_attached", 4),
        predicate(CRITIQUE_POSTED_ID, "critique_posted", 5),
        predicate(VERIFICATION_POSTED_ID, "verification_posted", 6),
        predicate(DECISION_POSTED_ID, "decision_posted", 6),
        predicate(ROUTE_PROPOSED_ID, "route_proposed", 5),
        predicate(ROUTE_DECIDED_ID, "route_decided", 5),
        predicate(ROUTER_UPDATE_POSTED_ID, "router_update_posted", 5),
    ]
}

pub trait ToPolFacts {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact>;
}

impl ToPolFacts for Polity {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            POLITY_DECLARED_ID,
            "polity_declared",
            4,
            vec![
                s(self.id.as_str()),
                s(&self.name),
                s(self.charter_id.as_str()),
                s(self.commons_id.as_str()),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for Guild {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            GUILD_DECLARED_ID,
            "guild_declared",
            5,
            vec![
                s(self.id.as_str()),
                s(self.polity_id.as_str()),
                s(&self.name),
                s(&self.purpose),
                s(self.charter_id.as_str()),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for AgentContract {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            AGENT_CONTRACTED_ID,
            "agent_contracted",
            7,
            vec![
                s(self.agent_id.as_str()),
                s(self.guild_id.as_str()),
                s(&self.role),
                string_list(&self.capabilities),
                string_list(&self.obligations),
                string_list(&self.permissions),
                s(&self.trust_domain),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for WorkObject {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            WORK_OBJECT_DECLARED_ID,
            "work_object_declared",
            6,
            vec![
                s(self.id.as_str()),
                s(self.polity_id.as_str()),
                s(kind_label(self.kind)),
                s(state_label(self.state)),
                s(&self.title),
                optional_id(self.owner_guild_id.as_ref().map(GuildId::as_str)),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for Claim {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            CLAIM_POSTED_ID,
            "claim_posted",
            6,
            vec![
                s(self.id.as_str()),
                s(self.work_object_id.as_str()),
                s(self.author_agent_id.as_str()),
                s(&self.statement),
                Value::F64(self.confidence as f64),
                string_list(self.evidence_ids.iter().map(EvidenceId::as_str)),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for EvidenceBundle {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            EVIDENCE_ATTACHED_ID,
            "evidence_attached",
            4,
            vec![
                s(self.id.as_str()),
                s(self.claim_id.as_str()),
                string_list(self.source_refs.iter().map(|source| source.uri.as_str())),
                optional_id(self.digest.as_deref()),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for Critique {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            CRITIQUE_POSTED_ID,
            "critique_posted",
            5,
            vec![
                s(self.id.as_str()),
                s(self.claim_id.as_str()),
                s(self.critic_agent_id.as_str()),
                s(critique_severity_label(self.severity)),
                s(&self.objection),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for Verification {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            VERIFICATION_POSTED_ID,
            "verification_posted",
            6,
            vec![
                s(self.id.as_str()),
                s(self.claim_id.as_str()),
                s(self.verifier_agent_id.as_str()),
                s(verdict_label(self.verdict)),
                Value::F64(self.confidence as f64),
                s(&self.rationale),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for Decision {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            DECISION_POSTED_ID,
            "decision_posted",
            6,
            vec![
                s(self.id.as_str()),
                s(self.work_object_id.as_str()),
                s(self.decided_by_agent_id.as_str()),
                s(&self.outcome),
                s(&self.rationale),
                string_list(self.supporting_claim_ids.iter().map(ClaimId::as_str)),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for RouteProposal {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            ROUTE_PROPOSED_ID,
            "route_proposed",
            5,
            vec![
                s(self.id.as_str()),
                s(self.work_object_id.as_str()),
                s(self.router_agent_id.as_str()),
                string_list(self.candidate_guild_ids.iter().map(GuildId::as_str)),
                s(&self.reason),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for RouteDecision {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            ROUTE_DECIDED_ID,
            "route_decided",
            5,
            vec![
                s(self.id.as_str()),
                s(self.proposal_id.as_str()),
                s(self.selected_guild_id.as_str()),
                s(self.selected_by_agent_id.as_str()),
                s(&self.reason),
            ],
            self.policy.clone(),
        )]
    }
}

impl ToPolFacts for RouterUpdate {
    fn to_pol_facts(&self) -> Vec<ExtensionalFact> {
        vec![fact(
            ROUTER_UPDATE_POSTED_ID,
            "router_update_posted",
            5,
            vec![
                s(self.id.as_str()),
                s(self.route_decision_id.as_str()),
                s(router_disposition_label(self.disposition)),
                Value::F64(self.utility_delta as f64),
                s(&self.regret_note),
            ],
            self.policy.clone(),
        )]
    }
}

fn predicate(id: PredicateId, name: &str, arity: usize) -> PredicateRef {
    PredicateRef {
        id,
        name: name.to_string(),
        arity,
    }
}

fn fact(
    predicate_id: PredicateId,
    predicate_name: &str,
    arity: usize,
    values: Vec<Value>,
    policy: Option<PolicyEnvelope>,
) -> ExtensionalFact {
    debug_assert_eq!(arity, values.len());
    ExtensionalFact {
        predicate: predicate(predicate_id, predicate_name, arity),
        values,
        policy,
        provenance: None,
    }
}

fn s(value: &str) -> Value {
    Value::String(value.to_string())
}

fn optional_id(value: Option<&str>) -> Value {
    value.map_or(Value::Null, s)
}

fn string_list<'a, I>(values: I) -> Value
where
    I: IntoIterator<Item = &'a str>,
{
    Value::List(values.into_iter().map(s).collect())
}

fn kind_label(kind: WorkObjectKind) -> &'static str {
    match kind {
        WorkObjectKind::Task => "task",
        WorkObjectKind::ClaimReview => "claim_review",
        WorkObjectKind::Incident => "incident",
        WorkObjectKind::ResearchBrief => "research_brief",
        WorkObjectKind::CodeChange => "code_change",
        WorkObjectKind::Benchmark => "benchmark",
        WorkObjectKind::DecisionRecord => "decision_record",
    }
}

fn state_label(state: WorkObjectState) -> &'static str {
    match state {
        WorkObjectState::Open => "open",
        WorkObjectState::Routed => "routed",
        WorkObjectState::Claimed => "claimed",
        WorkObjectState::InReview => "in_review",
        WorkObjectState::Accepted => "accepted",
        WorkObjectState::Retained => "retained",
        WorkObjectState::Rejected => "rejected",
        WorkObjectState::Closed => "closed",
    }
}

fn verdict_label(verdict: VerificationVerdict) -> &'static str {
    match verdict {
        VerificationVerdict::Supported => "supported",
        VerificationVerdict::Refuted => "refuted",
        VerificationVerdict::Inconclusive => "inconclusive",
        VerificationVerdict::NeedsHumanReview => "needs_human_review",
    }
}

fn critique_severity_label(severity: CritiqueSeverity) -> &'static str {
    match severity {
        CritiqueSeverity::Note => "note",
        CritiqueSeverity::Concern => "concern",
        CritiqueSeverity::Blocking => "blocking",
    }
}

fn router_disposition_label(disposition: RouterUpdateDisposition) -> &'static str {
    match disposition {
        RouterUpdateDisposition::Accepted => "accepted",
        RouterUpdateDisposition::Retained => "retained",
        RouterUpdateDisposition::Rejected => "rejected",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicate_catalog_is_stable() {
        let catalog = predicate_catalog();
        assert_eq!(catalog.len(), 12);
        assert_eq!(catalog[0].name, "polity_declared");
        assert_eq!(catalog[0].id, POLITY_DECLARED_ID);
        assert_eq!(catalog[11].name, "router_update_posted");
        assert_eq!(catalog[11].id, ROUTER_UPDATE_POSTED_ID);
        assert!(catalog.iter().all(|predicate| predicate.arity > 0));
    }

    #[test]
    fn work_claim_verification_decision_project_to_policy_bearing_facts() {
        let policy = PolicyEnvelope {
            capabilities: vec!["polity.write".to_string()],
            visibilities: vec!["internal".to_string()],
        };
        let work = WorkObject {
            id: WorkObjectId::new("work:1"),
            polity_id: PolityId::new("polity:aether"),
            kind: WorkObjectKind::ResearchBrief,
            state: WorkObjectState::Open,
            title: "Formalize AETHER-POL".to_string(),
            owner_guild_id: Some(GuildId::new("guild:architecture")),
            policy: Some(policy.clone()),
        };
        let claim = Claim {
            id: ClaimId::new("claim:1"),
            work_object_id: work.id.clone(),
            author_agent_id: AgentId::new("agent:architect"),
            statement: "AETHER-POL is a typed institutional vocabulary over the kernel.".to_string(),
            confidence: 0.91,
            evidence_ids: vec![EvidenceId::new("evidence:1")],
            policy: Some(policy.clone()),
        };
        let verification = Verification {
            id: VerificationId::new("verification:1"),
            claim_id: claim.id.clone(),
            verifier_agent_id: AgentId::new("agent:critic"),
            verdict: VerificationVerdict::Supported,
            confidence: 0.84,
            rationale: "Vocabulary projects to replayable extensional facts.".to_string(),
            policy: Some(policy.clone()),
        };
        let decision = Decision {
            id: DecisionId::new("decision:1"),
            work_object_id: work.id.clone(),
            decided_by_agent_id: AgentId::new("agent:lead"),
            outcome: "retain".to_string(),
            rationale: "Suitable as a post-v1 semantic layer.".to_string(),
            supporting_claim_ids: vec![claim.id.clone()],
            policy: Some(policy.clone()),
        };

        let facts = [
            work.to_pol_facts(),
            claim.to_pol_facts(),
            verification.to_pol_facts(),
            decision.to_pol_facts(),
        ]
        .concat();

        assert_eq!(facts.len(), 4);
        assert_eq!(facts[0].predicate.name, "work_object_declared");
        assert_eq!(facts[1].predicate.name, "claim_posted");
        assert_eq!(facts[2].predicate.name, "verification_posted");
        assert_eq!(facts[3].predicate.name, "decision_posted");
        assert!(facts.iter().all(|fact| fact.policy == Some(policy.clone())));
    }

    #[test]
    fn route_updates_record_accepted_retained_and_rejected_outcomes() {
        let accepted = RouterUpdate {
            id: RouterUpdateId::new("update:accepted"),
            route_decision_id: RouteDecisionId::new("route:1"),
            disposition: RouterUpdateDisposition::Accepted,
            utility_delta: 1.0,
            regret_note: "selected guild resolved the work object".to_string(),
            policy: None,
        };
        let retained = RouterUpdate {
            disposition: RouterUpdateDisposition::Retained,
            id: RouterUpdateId::new("update:retained"),
            ..accepted.clone()
        };
        let rejected = RouterUpdate {
            disposition: RouterUpdateDisposition::Rejected,
            id: RouterUpdateId::new("update:rejected"),
            ..accepted
        };

        let labels = [accepted, retained, rejected]
            .iter()
            .map(|update| update.to_pol_facts()[0].values[2].clone())
            .collect::<Vec<_>>();

        assert_eq!(labels, vec![s("accepted"), s("retained"), s("rejected")]);
    }
}
