"""
Optimized CDM Mapping Suggestions with Sector-Aware Contextual Mapping
Multi-sector support: BFSI (Banking, Insurance, Lending, Payments, Wealth, Treasury) + Retail + Consumer

Architecture:
1. Universal scoring framework (applies to all sectors)
2. Sector detection from table/column context
3. Sector-specific terminology and patterns
4. Dynamic prompt assembly based on detected sector
"""

# ═══════════════════════════════════════════════════════════════════════════════
# SECTOR TERMINOLOGY DICTIONARIES
# ═══════════════════════════════════════════════════════════════════════════════

SECTOR_PATTERNS = {
    "banking": {
        "keywords": [
            "account", "branch", "customer", "party", "cif", "deposit", "transaction",
            "ledger", "balance", "posting", "clearing", "settlement", "funds transfer",
            "current account", "savings", "checking", "debit", "credit"
        ],
        "entities": {
            "customer": ["customer", "party", "cif", "client", "account holder"],
            "contract": ["account", "arrangement", "facility", "agreement"],
            "location": ["branch", "banking center", "office", "sol"],
            "transaction": ["transaction", "posting", "movement", "transfer"],
            "product": ["product", "scheme", "account type", "plan"]
        },
        "amounts": {
            "balance": ["balance", "outstanding", "available balance", "ledger balance"],
            "transaction": ["amount", "value", "transaction amount"],
            "accrual": ["accrued", "interest accrued", "fee accrued"],
            "rate": ["interest rate", "rate", "APR", "margin"]
        },
        "dates": {
            "inception": ["account open date", "opening date", "activation date"],
            "maturity": ["maturity date", "closure date", "termination date"],
            "transaction": ["posting date", "value date", "transaction date", "business date"]
        },
        "status": ["account status", "lifecycle status", "active", "dormant", "closed"]
    },
    
    "insurance": {
        "keywords": [
            "policy", "policyholder", "insured", "premium", "claim", "coverage",
            "underwriting", "beneficiary", "risk", "loss", "endorsement",
            "sum assured", "maturity", "rider", "agent", "broker"
        ],
        "entities": {
            "customer": ["policyholder", "insured", "beneficiary", "proposer", "life assured"],
            "contract": ["policy", "contract", "certificate", "coverage"],
            "location": ["office", "branch", "servicing center", "regional office"],
            "transaction": ["claim", "premium payment", "endorsement", "surrender"],
            "product": ["policy type", "plan", "product", "scheme", "rider"]
        },
        "amounts": {
            "balance": ["sum assured", "sum insured", "coverage amount", "face value"],
            "transaction": ["claim amount", "premium amount", "surrender value"],
            "accrual": ["earned premium", "unearned premium", "reserve"],
            "rate": ["premium rate", "commission rate", "mortality rate"]
        },
        "dates": {
            "inception": ["policy start date", "effective date", "inception date", "issue date"],
            "maturity": ["maturity date", "expiry date", "termination date", "lapse date"],
            "transaction": ["claim date", "premium due date", "payment date"]
        },
        "status": ["policy status", "active", "lapsed", "surrendered", "matured", "inforce"]
    },
    
    "lending": {
        "keywords": [
            "loan", "borrower", "lender", "credit", "principal", "emi", "disbursement",
            "collateral", "security", "exposure", "npa", "delinquency", "repayment",
            "sanction", "drawdown", "installment", "overdue"
        ],
        "entities": {
            "customer": ["borrower", "obligor", "co-borrower", "guarantor", "applicant"],
            "contract": ["loan", "facility", "credit line", "limit", "sanction"],
            "location": ["loan center", "processing center", "branch"],
            "transaction": ["disbursement", "repayment", "emi collection", "charge-off"],
            "product": ["loan type", "product", "scheme", "facility type"]
        },
        "amounts": {
            "balance": ["outstanding principal", "outstanding balance", "exposure", "limit"],
            "transaction": ["disbursement amount", "emi amount", "repayment amount"],
            "accrual": ["accrued interest", "unpaid interest", "penalty accrued"],
            "rate": ["interest rate", "apr", "margin", "spread", "processing fee rate"]
        },
        "dates": {
            "inception": ["loan origination date", "sanction date", "disbursement date"],
            "maturity": ["maturity date", "final payment date", "closure date"],
            "transaction": ["repayment date", "emi date", "due date", "overdue date"]
        },
        "status": ["loan status", "active", "closed", "npa", "written off", "restructured"]
    },
    
    "payments": {
        "keywords": [
            "card", "cardholder", "merchant", "terminal", "pos", "authorization",
            "settlement", "interchange", "acquiring", "issuing", "chargeback",
            "pan", "transaction", "payment", "acquirer", "processor"
        ],
        "entities": {
            "customer": ["cardholder", "merchant", "payee", "payer", "customer"],
            "contract": ["card account", "merchant agreement", "card"],
            "location": ["terminal", "pos", "atm", "merchant location", "acquiring location"],
            "transaction": ["authorization", "settlement", "payment", "transfer", "chargeback"],
            "product": ["card type", "card product", "payment type", "channel"]
        },
        "amounts": {
            "balance": ["credit limit", "outstanding", "available credit"],
            "transaction": ["transaction amount", "settlement amount", "authorization amount"],
            "accrual": ["interest charged", "fee accrued"],
            "rate": ["interchange rate", "mdr", "fee rate", "interest rate"]
        },
        "dates": {
            "inception": ["card issue date", "activation date", "account open date"],
            "maturity": ["card expiry", "expiration date"],
            "transaction": ["authorization date", "settlement date", "transaction date"]
        },
        "status": ["card status", "active", "blocked", "expired", "cancelled"]
    },
    
    "wealth": {
        "keywords": [
            "portfolio", "investment", "nav", "fund", "investor", "unit holder",
            "mutual fund", "aum", "redemption", "subscription", "folio",
            "asset allocation", "dividend", "capital gains", "sip"
        ],
        "entities": {
            "customer": ["investor", "unit holder", "client", "account holder", "subscriber"],
            "contract": ["portfolio", "investment account", "folio", "fund account"],
            "location": ["investment center", "branch", "office"],
            "transaction": ["subscription", "redemption", "switch", "dividend payment"],
            "product": ["fund", "scheme", "plan", "portfolio type", "asset class"]
        },
        "amounts": {
            "balance": ["portfolio value", "nav", "aum", "invested amount", "current value"],
            "transaction": ["subscription amount", "redemption amount", "dividend"],
            "accrual": ["accrued dividend", "accrued interest"],
            "rate": ["return rate", "expense ratio", "nav growth", "yield"]
        },
        "dates": {
            "inception": ["account opening date", "first investment date", "subscription date"],
            "maturity": ["maturity date", "lock-in expiry"],
            "transaction": ["transaction date", "nav date", "allotment date"]
        },
        "status": ["account status", "active", "dormant", "closed"]
    },
    
    "treasury": {
        "keywords": [
            "deal", "position", "counterparty", "settlement", "valuation", "exposure",
            "forex", "derivative", "hedge", "mark to market", "collateral",
            "trading", "nostro", "vostro", "swap", "forward"
        ],
        "entities": {
            "customer": ["counterparty", "trading partner", "client", "correspondent bank"],
            "contract": ["deal", "trade", "contract", "agreement", "facility"],
            "location": ["trading desk", "treasury center", "dealing room"],
            "transaction": ["trade", "settlement", "payment", "transfer", "novation"],
            "product": ["instrument", "product type", "derivative type", "currency pair"]
        },
        "amounts": {
            "balance": ["position", "exposure", "notional", "market value", "fair value"],
            "transaction": ["deal amount", "settlement amount", "payment amount"],
            "accrual": ["accrued interest", "mark to market", "unrealized pnl"],
            "rate": ["exchange rate", "interest rate", "swap rate", "forward rate"]
        },
        "dates": {
            "inception": ["trade date", "deal date", "value date"],
            "maturity": ["maturity date", "settlement date", "expiry date"],
            "transaction": ["settlement date", "payment date", "fixing date"]
        },
        "status": ["deal status", "active", "settled", "cancelled", "matured"]
    },
    
    "retail": {
        "keywords": [
            "store", "product", "sku", "sale", "invoice", "customer", "receipt",
            "inventory", "merchandise", "pos", "cashier", "discount", "loyalty",
            "promotion", "basket", "transaction", "checkout"
        ],
        "entities": {
            "customer": ["customer", "member", "loyalty member", "shopper"],
            "contract": ["loyalty account", "membership", "card"],
            "location": ["store", "outlet", "shop", "location", "branch"],
            "transaction": ["sale", "invoice", "receipt", "transaction", "order"],
            "product": ["product", "item", "sku", "article", "merchandise"]
        },
        "amounts": {
            "balance": ["loyalty points", "reward balance", "stored value"],
            "transaction": ["sale amount", "invoice amount", "line amount", "total"],
            "accrual": ["loyalty earned", "points accrued"],
            "rate": ["discount rate", "markup", "margin", "commission rate"]
        },
        "dates": {
            "inception": ["store opening date", "membership date", "first purchase date"],
            "maturity": ["membership expiry", "promotion end date"],
            "transaction": ["sale date", "invoice date", "transaction date", "business date"]
        },
        "status": ["store status", "product status", "active", "inactive", "discontinued"]
    },
    
    "consumer": {
        "keywords": [
            "customer", "order", "purchase", "product", "delivery", "shipment",
            "subscription", "service", "campaign", "segment", "preference"
        ],
        "entities": {
            "customer": ["customer", "consumer", "subscriber", "user", "member"],
            "contract": ["subscription", "membership", "account", "service agreement"],
            "location": ["delivery location", "warehouse", "fulfillment center", "region"],
            "transaction": ["order", "purchase", "delivery", "return", "cancellation"],
            "product": ["product", "service", "offering", "subscription plan"]
        },
        "amounts": {
            "balance": ["account balance", "credit balance", "reward points"],
            "transaction": ["order value", "purchase amount", "refund amount"],
            "accrual": ["revenue recognized", "deferred revenue"],
            "rate": ["discount rate", "loyalty rate", "conversion rate"]
        },
        "dates": {
            "inception": ["registration date", "subscription start", "first order date"],
            "maturity": ["subscription end", "contract expiry"],
            "transaction": ["order date", "delivery date", "purchase date"]
        },
        "status": ["customer status", "order status", "active", "inactive", "churned"]
    }
}


# ═══════════════════════════════════════════════════════════════════════════════
# SECTOR DETECTION FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

def detect_sector(app_info: dict) -> str:
    """
    Detect business sector from table name, description, and column context.
    
    Args:
        app_info: Dictionary with csv_table_name, csv_table_description, 
                  csv_column_name, csv_column_description
    
    Returns:
        Sector name (banking, insurance, lending, payments, wealth, treasury, retail, consumer)
        Defaults to 'banking' if unclear
    """
    # Combine all context for keyword matching
    context = " ".join([
        str(app_info.get('csv_table_name', '')),
        str(app_info.get('csv_table_description', '')),
        str(app_info.get('csv_column_name', '')),
        str(app_info.get('csv_column_description', ''))
    ]).lower()
    
    # Score each sector based on keyword presence
    sector_scores = {}
    for sector, patterns in SECTOR_PATTERNS.items():
        score = 0
        for keyword in patterns['keywords']:
            if keyword.lower() in context:
                score += 1
        sector_scores[sector] = score
    
    # Return sector with highest score, default to banking if all zeros
    detected = max(sector_scores, key=sector_scores.get)
    if sector_scores[detected] == 0:
        detected = 'banking'  # Default fallback
    
    return detected


# ═══════════════════════════════════════════════════════════════════════════════
# FUNDAMENTAL PRINCIPLES - READ FIRST (Applies to All Sectors)
# ═══════════════════════════════════════════════════════════════════════════════

FUNDAMENTAL_PRINCIPLES = """
═══════════════════════════════════════════════════════════════════════════════
FUNDAMENTAL PRINCIPLES (READ FIRST - APPLIES TO ALL MAPPINGS)
═══════════════════════════════════════════════════════════════════════════════

**PRINCIPLE 1: DESCRIPTIONS OVER NAMES (MOST CRITICAL)**
Column and Table names can be misleading, abbreviated, cryptic, or domain-specific. DESCRIPTIONS reveal the true semantic meaning and business purpose. ALWAYS prioritize descriptions in your analysis:

✓ HIGH MATCH: Descriptions show same functional purpose → High score even if names completely differ
  Example: "ACO_AMT_ACR_RND" (name unclear) with description "Interest earned but not yet paid" 
  matches CDM "Accrued Interest Amount" with description "Interest accumulated over time period" = 85+ score

✗ LOW MATCH: Names look similar but descriptions show different purposes → Low score
  Example: "customer_status" matches CDM "Customer Status" by name BUT if source description says 
  "risk rating classification" and CDM says "lifecycle state (active/closed)" = 30 score max

**PRINCIPLE 2: FUNCTIONAL EQUIVALENCE ACROSS SECTORS**
Different sectors use different terminology for equivalent concepts. Match by FUNCTIONAL ROLE, not sector-specific jargon:
- "Customer" (Banking) = "Policyholder" (Insurance) = "Borrower" (Lending) = "Shopper" (Retail) = Functional role: PRIMARY PARTY
- "Account" (Banking) = "Policy" (Insurance) = "Loan" (Lending) = "Membership" (Retail) = Functional role: CONTRACT/AGREEMENT
- "Transaction" (Banking) = "Claim" (Insurance) = "Disbursement" (Lending) = "Sale" (Retail) = Functional role: BUSINESS EVENT

Recognize these equivalences by reading descriptions, not by forcing name matches.

**PRINCIPLE 3: ABBREVIATIONS AND CRYPTIC NAMES REQUIRE DESCRIPTION DECODING**
Industry data often uses abbreviations (CIF, EMI, NAV, COF, ACO, SKU, POS, etc.) that are meaningless without context:
- CIF = Customer Information File (customer identifier)
- EMI = Equated Monthly Installment (payment amount)
- NAV = Net Asset Value (portfolio value)
- ACO = Accrual (accumulated amount)
- SKU = Stock Keeping Unit (product identifier)
- POS = Point of Sale (sales location/transaction point)

→ These CANNOT be matched by name patterns - you MUST read the description to understand what they represent

**PRINCIPLE 4: TABLE CONTEXT SUPPORTS, DESCRIPTIONS DICTATE**
Use table descriptions to understand broader context, but the COLUMN description is the primary authority:
- If column description clearly states its purpose, that overrides any assumptions from table name/type
- If table is "customer_master" but column description says "branch where account was opened", 
  this is a location reference, not a customer attribute
"""


# ═══════════════════════════════════════════════════════════════════════════════
# COMPREHENSIVE REASONING FRAMEWORK
# ═══════════════════════════════════════════════════════════════════════════════

REASONING_FRAMEWORK = """
═══════════════════════════════════════════════════════════════════════════════
ENHANCED REASONING FRAMEWORK - Follow this step-by-step approach:
═══════════════════════════════════════════════════════════════════════════════

STEP 1: COMPREHENSIVE CONTEXT ANALYSIS
- Read and analyze the APPLICATION column DESCRIPTION carefully (primary source of truth)
- Read the APPLICATION column NAME (secondary, may be abbreviated or cryptic)
- Analyze the APPLICATION table description to understand the broader data domain
- For each CDM candidate:
  * Read CDM column DEFINITION/DESCRIPTION carefully (primary source of truth)
  * Read CDM table definition to understand the target domain context
  * Note the CDM column NAME (may use different sector terminology)
  * How the CDM column fits within its table's business purpose
- Compare application context with CDM context at both column and table levels
- **IDENTIFY FUNCTIONAL ROLES FROM DESCRIPTIONS**: Categorize columns by their functional purpose 
  (identifier for what?, amount measuring what?, date of what event?, status of what lifecycle?, code classifying what?)

STEP 1.5: CROSS-SECTOR FUNCTIONAL MATCHING (CRITICAL FOR ACCURACY)
When source and CDM use different terminologies (e.g., banking terms vs insurance terms vs retail terms):
1. **Ignore naming differences** - Focus on functional purpose revealed in descriptions
2. **Focus on DATA PURPOSE extracted from descriptions**:
   - What business question does this data answer?
   - What calculation or process uses this data?
   - What business rule or policy does it enforce?
3. **Match by FUNCTIONAL CATEGORY**:
   - Both are identifiers → Compare WHAT they identify (entity type)
   - Both are amounts → Compare WHAT they measure (revenue, cost, balance)
   - Both are dates → Compare WHAT event they mark (start, end, effective)
   - Both are statuses → Compare WHAT they classify (lifecycle, approval, risk)
4. **Description is KING**: When descriptions show identical purpose despite naming mismatch, consider high match
5. **Table context SUPPORTS, doesn't DICTATE**: If column purpose aligns, minor table domain differences are acceptable

STEP 2: MULTI-LEVEL SEMANTIC ALIGNMENT
- Column-level semantic matching: App column ↔ CDM column definitions
- Table-level context matching: App table purpose ↔ CDM table purpose  
- Business domain alignment: App business use case ↔ CDM business use case
- Data granularity compatibility: Transaction/Entity/Temporal level alignment
- Cross-domain semantic translation: Recognize equivalent concepts across sectors
  * **People/organizations**: customer ≈ party ≈ client ≈ policyholder ≈ borrower ≈ account holder ≈ shopper ≈ member
    (All identify individuals or entities in business relationships)
  * **Contracts/agreements**: account ≈ policy ≈ loan ≈ agreement ≈ contract ≈ facility ≈ membership ≈ subscription
    (All represent formal business arrangements with terms and conditions)
  * **Business events**: transaction ≈ claim ≈ payment ≈ disbursement ≈ settlement ≈ posting ≈ sale ≈ order ≈ delivery
    (All record discrete business activities with financial or operational impact)
  * **Locations**: branch ≈ office ≈ facility ≈ servicing center ≈ point of service ≈ store ≈ outlet ≈ warehouse
    (All identify physical or virtual service delivery points)
  * **Financial amounts**: balance ≈ outstanding ≈ exposure ≈ liability ≈ reserve ≈ accrual ≈ inventory value
    (All represent monetary values, match by WHAT they measure - owed, earned, committed, available)
  * Focus on SEMANTIC ROLE and BUSINESS PURPOSE, not domain-specific terminology
"""


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTIONAL ROLE PATTERNS (Cross-Domain Universals)
# ═══════════════════════════════════════════════════════════════════════════════

FUNCTIONAL_ROLE_PATTERNS = """
═══════════════════════════════════════════════════════════════════════════════
STEP 3: FUNCTIONAL ROLE PATTERNS (Cross-Domain Universals)
═══════════════════════════════════════════════════════════════════════════════

**Functional Role Patterns - Match based on WHAT columns represent, not names:**

┌─────────────────────────────────────────────────────────────────────────────┐
│ IDENTIFIERS (PK/FK) - Match based on WHAT they identify                     │
├─────────────────────────────────────────────────────────────────────────────┤
│ • All person/org identifiers: Match any column identifying individuals or   │
│   entities regardless of domain term                                        │
│   (customer_id, party_id, client_num, policyholder_id, borrower_id, CIF,    │
│   UCN, entity_id, shopper_id, member_id - focus on "identifies a person")   │
│                                                                              │
│ • All contract identifiers: Match any column identifying business           │
│   agreements/contracts                                                       │
│   (account_no, policy_num, loan_id, agreement_ref, contract_id,             │
│   subscription_id, membership_no - focus on "identifies a contractual       │
│   arrangement")                                                              │
│                                                                              │
│ • All transaction identifiers: Match any column identifying discrete        │
│   business events                                                            │
│   (transaction_id, payment_ref, claim_num, posting_id, order_id, sale_id,   │
│   invoice_no - focus on "identifies a business event")                      │
│                                                                              │
│ CRITICAL: Don't force matches by name patterns like "*_ID" - evaluate WHAT  │
│ is being identified from description                                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ MONETARY AMOUNTS - Match based on PURPOSE (revenue, cost, balance, fee)     │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Accumulated financial values: Amounts that build up over time regardless  │
│   of specific category                                                       │
│   (accrued_interest, earned_premium, outstanding_principal, unpaid_balance, │
│   inventory_value - focus on "accumulated monetary value")                  │
│                                                                              │
│ • Point-in-time amounts: Snapshot financial values                          │
│   (current_balance, outstanding_amount, reserve_amount, available_balance,  │
│   stock_value - focus on "monetary value at specific moment")               │
│                                                                              │
│ • Transactional amounts: Money changing hands in a specific event           │
│   (payment_amount, disbursement_amt, claim_payment, sale_amount, order_val, │
│   refund_amount - focus on "money involved in single transaction")          │
│                                                                              │
│ CRITICAL: Match by WHAT the amount represents (liability vs asset, owed vs  │
│ paid, principal vs interest) not just that it's a currency field            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ DATES - Match based on BUSINESS MEANING (start, end, effective, transaction)│
├─────────────────────────────────────────────────────────────────────────────┤
│ • Inception dates: When something begins (contract start, account opening,  │
│   policy effective date, membership start, first purchase)                  │
│   - Focus on "marks the beginning of a relationship or agreement"           │
│                                                                              │
│ • Termination dates: When something ends (maturity, closure, expiration,    │
│   cancellation, subscription_end)                                            │
│   - Focus on "marks the conclusion or end of validity"                      │
│                                                                              │
│ • Transaction/event dates: When a business event occurred                   │
│   - Focus on "timestamp of a discrete business activity"                    │
│                                                                              │
│ • Reporting dates: As-of dates for snapshots                                │
│   - Focus on "point in time for which data is valid"                        │
│                                                                              │
│ CRITICAL: Match by temporal MEANING not just data type - "effective_date"   │
│ of policy inception should NOT match "last_payment_date" just because both  │
│ are dates                                                                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ STATUS/CLASSIFICATION CODES - Match based on WHAT they classify              │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Lifecycle states: Codes indicating where an entity is in its lifecycle    │
│   (active, dormant, closed, suspended, matured, terminated, pending,        │
│   cancelled, shipped, delivered)                                             │
│   - Focus on "where in its lifecycle is this entity/contract/account"       │
│                                                                              │
│ • Risk/quality classifications: Ratings, grades, scores assessing risk or   │
│   quality                                                                    │
│   (risk rating, credit grade, performance tier, quality segment, collection │
│   status, product rating)                                                    │
│   - Focus on "how is risk/quality/creditworthiness assessed"                │
│                                                                              │
│ • Category/type codes: Codes classifying the type or category of entity/    │
│   product/service                                                            │
│   (product type, account category, transaction type, service code, policy   │
│   class, item_category)                                                      │
│   - Focus on "what category or type does this belong to"                    │
│                                                                              │
│ • Geographic/organizational codes: Codes identifying organizational units   │
│   or locations                                                               │
│   (region code, branch code, division, zone, district, servicing office,    │
│   store_region, warehouse_zone)                                              │
│   - Focus on "which organizational unit or geographic location"             │
│                                                                              │
│ CRITICAL: Match by classification PURPOSE - account lifecycle status should │
│ NOT match customer risk rating even though both use status codes. Verify    │
│ WHAT is being classified from descriptions                                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ RATES/PERCENTAGES - Match based on WHAT they measure                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Pricing rates: Interest rates, discount rates, markup percentages         │
│   - Focus on "rate applied to calculate cost or return"                     │
│                                                                              │
│ • Performance metrics: Utilization rates, conversion rates, success rates   │
│   - Focus on "percentage measuring performance or efficiency"               │
│                                                                              │
│ CRITICAL: Interest rate (pricing) should NOT match utilization rate         │
│ (performance metric)                                                         │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ DESCRIPTIVE TEXT - Match based on WHAT they describe                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Entity descriptions: Product names, account descriptions, policy          │
│   descriptions, item names                                                   │
│   - Focus on "what is being described" (the subject)                        │
│                                                                              │
│ • Event descriptions: Transaction descriptions, claim descriptions, payment │
│   descriptions, order notes                                                  │
│   - Focus on "describes a business event or activity"                       │
│                                                                              │
│ CRITICAL: Account description should NOT match transaction narrative just   │
│ because both contain text                                                    │
└─────────────────────────────────────────────────────────────────────────────┘

**DESCRIPTION PRIORITY RULE**:
When column **descriptions** show functional equivalence, override naming dissimilarity:
- Award full semantic points if descriptions reveal same functional purpose
- Names like "ACO_AMT_ACR_RND" vs "total_revenue" can score 35+ if descriptions show both capture accumulated financial amounts
"""


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE FUNCTION RECOGNITION AND TYPE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

TABLE_CLASSIFICATION = """
═══════════════════════════════════════════════════════════════════════════════
TABLE FUNCTION RECOGNITION (Cross-Domain)
═══════════════════════════════════════════════════════════════════════════════

**Table Function Recognition - Identify by FUNCTION, not naming patterns:**

┌─────────────────────────────────────────────────────────────────────────────┐
│ MASTER/REFERENCE DATA - Contains entity definitions, slowly changing        │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Naming indicators: *_master, *_dim, *_ref, *_info, *_catalog, dim_*,      │
│   mstr_*, *_lookup                                                           │
│                                                                              │
│ • Functional indicators: Contains unique entity identifiers (PK) and        │
│   descriptive attributes                                                     │
│                                                                              │
│ • Content describes "WHAT/WHO/WHERE" - products, customers, locations,      │
│   policies, accounts                                                         │
│                                                                              │
│ • Examples across sectors:                                                  │
│   - Banking: Account definitions, customer profiles, product catalogs,      │
│     branch details                                                           │
│   - Insurance: Policy master, policyholder info, coverage definitions,      │
│     agent registry                                                           │
│   - Lending: Loan terms reference, borrower profiles, collateral registry   │
│   - Payments: Merchant master, card product catalog, terminal registry      │
│   - Retail: Product catalog, store master, customer master, supplier master │
│   - Consumer: Service catalog, membership master, campaign registry         │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ TRANSACTION/EVENT/FACT DATA - Records business events, high cardinality     │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Naming indicators: *_txn, *_fact, *_trans, *_event, *_activity, fact_*,   │
│   *_header, *_line, *_detail, *_hist                                         │
│                                                                              │
│ • Functional indicators: Contains timestamps, monetary amounts, references  │
│   to master data (FKs)                                                       │
│                                                                              │
│ • Content describes "WHEN/HOW MUCH" - business transactions, events,        │
│   measurements                                                               │
│                                                                              │
│ • Examples across sectors:                                                  │
│   - Banking: Transaction logs, interest accruals, fee postings, balance     │
│     snapshots                                                                │
│   - Insurance: Claims transactions, premium payments, policy endorsements   │
│   - Lending: Disbursements, EMI collections, charge-offs, payment schedules │
│   - Payments: Authorization events, settlement batches, chargeback records  │
│   - Retail: Sales transactions, invoice header, order detail, inventory     │
│     movements                                                                │
│   - Consumer: Order transactions, delivery events, subscription renewals    │
└─────────────────────────────────────────────────────────────────────────────┘

**Table Purpose Determination**:
1. Read table **description** (most authoritative)
2. Look for keywords: "master", "reference", "fact", "transaction", "accrual", "event", "ledger"
3. Analyze columns: Many FKs + amounts + dates = transaction; PK + attributes = master
4. **Trust description over naming pattern**

═══════════════════════════════════════════════════════════════════════════════
TABLE TYPE CLASSIFICATION AND ALIGNMENT (CRITICAL FOR CORRECT MAPPING)
═══════════════════════════════════════════════════════════════════════════════

**CRITICAL Mapping Rules by Table Type**:

✅ Dimension → Dimension: STRONGLY PREFERRED (+10 bonus points)
   Reason: Master data identifiers should map to master data, maintaining data governance

❌ Dimension → Fact Table FK: MAJOR PENALTY (-25 points)
   Reason: A master table's primary identifier should NOT map to a foreign key in a transactional table
   This represents a fundamental misunderstanding of the data's purpose

✅ Fact → Fact: PREFERRED for transactional columns (+10 bonus points)
   Reason: Transaction metrics, events, and measures align within same context

✅ Fact FK → Dimension PK: ACCEPTABLE when mapping FK to its source (+5 bonus points)
   Reason: Foreign keys in transactions should reference master data identifiers

**Decision Framework**:
1. Identify source table type from naming pattern AND table description (description is authoritative)
2. Identify CDM candidate table type from naming pattern AND table definition
3. Apply appropriate bonus or penalty based on table type alignment
4. If source is dimension/master table, HEAVILY penalize fact/transaction table candidates
5. Prioritize candidates from same table type category
6. When uncertain, table description takes precedence over naming pattern
"""


# ═══════════════════════════════════════════════════════════════════════════════
# KEY RELATIONSHIP ANALYSIS (PK/FK/Composite Keys)
# ═══════════════════════════════════════════════════════════════════════════════

RELATIONSHIP_ANALYSIS = """
═══════════════════════════════════════════════════════════════════════════════
KEY RELATIONSHIP ANALYSIS (If given in the CDM)
═══════════════════════════════════════════════════════════════════════════════

**Primary Key (PK) Handling:**
- If source column is a Primary Key (PK), prioritize CDM candidates that represent primary identifiers in master/dimension tables
- PK columns define the unique entity - match to CDM entity identifiers, not transaction references

**Foreign Key (FK) Handling - Analyze the referenced entity context and relationship purpose:**
- FK to entity/party tables → prioritize CDM entity/party identifiers
- FK to transactional tables → prioritize CDM transaction/event references
- FK to product/offering tables → prioritize CDM product/service identifiers
- FK to location/resource tables → prioritize CDM location/resource identifiers (facilities, sites, branches, stores)
- FK to classification tables → prioritize CDM category/type/classification identifiers
- FK to temporal tables → prioritize CDM date/time/period identifiers

**Composite Key Handling (multi-column PK/FK):**
- Analyze the combination's business meaning (e.g., entity_id + period identifies time-series data)
- Each component should map to its individual semantic purpose in CDM
- Composite keys often represent: many-to-many relationships, hierarchical structures, temporal uniqueness, or multi-dimensional granularity
- Map each component independently while noting the composite relationship in reasoning
- Apply +5 bonus points when composite key components maintain relational consistency in CDM
- Examples across domains:
  * Retail: (store_id, date) → map components separately to STORE_ID and BUSINESS_DATE
  * Banking: (account_id, transaction_date) → map to ACCOUNT_REFERENCE and TRANSACTION_DATE
  * Insurance: (policy_id, endorsement_seq) → map to POLICY_NUMBER and MODIFICATION_SEQUENCE

**Critical Rules:**
- ID columns should map based on WHAT they identify (semantic purpose), not just that they're IDs
- Consider referential integrity: mapped column should maintain similar relational role in CDM
- For composite keys, verify that all components together would preserve the original relationship cardinality in CDM
- Relationship context is MORE important than naming similarity
"""


# ═══════════════════════════════════════════════════════════════════════════════
# ENTITY IDENTIFICATION GUIDANCE (Multi-Sector)
# ═══════════════════════════════════════════════════════════════════════════════

ENTITY_IDENTIFICATION = """
═══════════════════════════════════════════════════════════════════════════════
ENTITY IDENTIFICATION GUIDANCE (Multi-Sector)
═══════════════════════════════════════════════════════════════════════════════

**FUNCTIONAL EQUIVALENCE RULE**: IDs identifying the same entity TYPE should match despite different sector terminology

**CRITICAL**: Analyze the entity TYPE from column DESCRIPTION and table context, NOT from abbreviations or naming conventions
- Column "FAC_CODE" could be facility code (location) OR facility/credit line code (product) - read the description!
- Column "PARTY_ID" could be any person or organization - check if it's customer, vendor, guarantor, etc.

┌─────────────────────────────────────────────────────────────────────────────┐
│ PEOPLE/ORGANIZATIONS - Map to appropriate CDM party/customer/entity IDs     │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Customers/Clients:                                                         │
│   - Banking: customer_id, party_id, CIF, client_number                      │
│   - Insurance: policyholder_id, insured_id, beneficiary_id, claimant_id     │
│   - Lending: borrower_id, obligor_id, applicant_id, co-borrower_id          │
│   - Payments: cardholder_id, merchant_id, acquirer_id                       │
│   - Wealth: investor_id, account_holder_id, unit_holder_id                  │
│   - Retail: customer_id, member_id, shopper_id, loyalty_member_id           │
│   - Consumer: subscriber_id, user_id, consumer_id                           │
│   → All identify THE PRIMARY PERSON/ORGANIZATION in the business            │
│     relationship                                                             │
│                                                                              │
│ • Business Partners/Counterparties:                                          │
│   - vendor_id, supplier_id, correspondent_bank_id, reinsurer_id, broker_id, │
│     agent_id, distributor_id                                                 │
│   → All identify EXTERNAL BUSINESS PARTNERS or counterparties               │
│                                                                              │
│ • Internal Resources:                                                        │
│   - employee_id, relationship_manager_id, underwriter_id, loan_officer_id,  │
│     agent_code, sales_rep_id                                                 │
│   → All identify INTERNAL STAFF serving customers                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ PRODUCTS/SERVICES/CONTRACTS - Map to appropriate CDM product/contract IDs   │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Core Financial Products/Agreements:                                        │
│   - Banking: account_number, account_id, arrangement_id, deposit_account    │
│   - Insurance: policy_number, policy_id, contract_number, certificate_num   │
│   - Lending: loan_account, loan_id, facility_id, limit_id, credit_line      │
│   - Payments: card_number, card_account_id, PAN                             │
│   - Wealth: portfolio_id, fund_code, investment_account, folio_number       │
│   - Retail: membership_id, loyalty_card_no, subscription_id                 │
│   - Consumer: subscription_id, service_agreement_id, contract_ref            │
│   → All represent SPECIFIC INSTANCES of products or contracts               │
│                                                                              │
│ • Product Types/Categories:                                                  │
│   - product_code, product_type, scheme_code, plan_code, account_type,       │
│     policy_type, item_category, service_type                                 │
│   → All identify THE CATEGORY/TYPE of product (not a specific instance)    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ LOCATIONS/ORGANIZATIONAL UNITS - Map to appropriate CDM location/org IDs    │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Physical/Service Locations:                                                │
│   - branch_code, branch_id, office_code, servicing_center, home_branch,     │
│     sol_id, store_id, outlet_code, warehouse_id, facility_code,             │
│     distribution_center                                                      │
│   - region_code, zone_code, cluster_id, district_code, circle_code          │
│   → All identify WHERE business is conducted or WHERE services are          │
│     delivered                                                                │
│                                                                              │
│ • Processing/Back Office:                                                    │
│   - processing_center_id, data_center_code, ops_unit_id                     │
│   → All identify OPERATIONAL processing locations                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ TRANSACTIONS/EVENTS - Map to appropriate CDM transaction/event IDs          │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Unique Transaction IDs:                                                    │
│   - Banking: transaction_id, reference_number, sequence_number, posting_ref │
│   - Insurance: claim_number, claim_id, endorsement_number, premium_receipt  │
│   - Lending: disbursement_id, repayment_id, payment_reference, EMI_number   │
│   - Payments: authorization_code, settlement_id, transaction_reference, UTR │
│   - Wealth: order_id, trade_id, transaction_number, allotment_id            │
│   - Retail: invoice_no, sale_id, order_id, receipt_number, transaction_id   │
│   - Consumer: order_number, delivery_id, shipment_tracking, event_id        │
│   → All uniquely identify A SPECIFIC BUSINESS EVENT or transaction instance │
└─────────────────────────────────────────────────────────────────────────────┘

**Critical Matching Rules:**
- Match entity TYPE and BUSINESS CONTEXT, not just naming similarity or abbreviations
- Customer ID (banking) should match Policyholder ID (insurance) - both identify the primary customer entity
- Branch Code (banking) should match Servicing Office (insurance) or Store ID (retail) - both identify service locations
- Account Number (banking) should match Policy Number (insurance) or Membership ID (retail) - both identify contract instances
- Apply +10 bonus when entity type semantically aligns with CDM entity category
- Apply -15 penalty when forcing an entity identifier into wrong category (e.g., location_code → customer_id)
- When CDM has sector-specific entity identifiers, use DESCRIPTION and TABLE CONTEXT to determine fit
- DO NOT force all IDs to map to the same CDM identifier - semantic purpose must align
"""


# ═══════════════════════════════════════════════════════════════════════════════
# COMPREHENSIVE CONTEXT EVALUATION RULES
# ═══════════════════════════════════════════════════════════════════════════════

CONTEXT_EVALUATION = """
═══════════════════════════════════════════════════════════════════════════════
COMPREHENSIVE CONTEXT EVALUATION RULES
═══════════════════════════════════════════════════════════════════════════════

**Table Context Priority:**
- If app table description indicates specific business domain (loans, deposits, policies, sales, etc.), 
  prioritize CDM candidates from compatible business domains (+15 bonus)
- Ensure mapped CDM column makes logical sense within its CDM table context
- Cross-domain matches (banking→insurance) are acceptable if functional purpose aligns

**Definition Quality Assessment:**
- **Descriptions are PRIMARY source of truth** for semantic alignment, especially in cross-domain scenarios
- When both app and CDM have detailed descriptions, analyze FUNCTIONAL PURPOSE revealed in text:
  * What does the column store? (identifier, amount, description, code, flag)
  * What business process uses it? (calculation, lookup, validation, reporting)
  * What decisions rely on it? (pricing, approval, classification, measurement)

- If CDM column definition is generic or brief, extract purpose from table definition context
- If app column description is detailed, extract ALL semantic meaning - it's your best guide

- **CRITICAL RULES FOR DESCRIPTION vs NAME CONFLICTS**:
  * Naming mismatch + description match = HIGH SCORE (descriptions override names)
    Example: "ACO_AMT_ACR_RND" matches "Accrued_Interest_Amount" if both descriptions say "interest earned but not paid"
  * Naming match + description mismatch = LOW SCORE (names can be misleading)
    Example: "customer_status" matches "Customer_Status" by name but if one describes "risk rating" and other 
    describes "lifecycle state" → LOW score despite naming similarity

- Missing or vague definitions should lower confidence but not eliminate candidates if:
  * Functional role is clear (e.g., all amount fields serve similar purpose)
  * Table context strongly supports the match
  * Column name patterns suggest equivalence (e.g., *_amt, *_date, *_id, *_code)

- Cross-reference column definitions with table definitions for validation
- Domain-specific terminology should be translated to underlying FUNCTIONAL concepts:
  * "Accrued Interest" (banking) → "Accumulated monetary value over period" (universal concept)
  * "Loyalty Tier" (retail) → "Customer classification based on behavior" (universal concept)
  * "Policy Inception Date" (insurance) → "Contract start date" (universal concept)
  * "Sum Assured" (insurance) → "Guaranteed payment amount" (universal concept)

- **Detailed descriptions ALWAYS override naming conventions** in determining semantic fit

**Column-Table Coherence:**
- Flag mappings where column semantics conflict with table purpose
- Example problem: Account balance column from transaction table should NOT map to account master's 
  opening balance - different granularities despite similar semantic meaning
- Apply -10 penalty for fundamental coherence conflicts (not just naming mismatches)
"""


# ═══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL SCORING FRAMEWORK (Common to All Sectors)
# ═══════════════════════════════════════════════════════════════════════════════

UNIVERSAL_SCORING_FRAMEWORK = """
═══════════════════════════════════════════════════════════════════════════════
STEP 4: SCORING BREAKDOWN (100 points total)
═══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│ A. SEMANTIC/FUNCTIONAL MATCH (40 points max - INCREASED WEIGHT)             │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Exact functional match (descriptions show same purpose): 35-40 pts        │
│   Example: "monetary accumulation over time" matches "monetary              │
│   accumulation" - focus on WHAT it represents (earned but unpaid amounts)   │
│                                                                              │
│ • Strong conceptual alignment (same data role, similar context): 25-34 pts  │
│   Focus on: What does the data REPRESENT? What BUSINESS DECISION does it    │
│   support?                                                                   │
│                                                                              │
│ • Partial semantic overlap (related category, different granularity):       │
│   15-24 pts                                                                  │
│                                                                              │
│ • Weak alignment (tangentially related): 5-14 pts                           │
│                                                                              │
│ • No meaningful relationship: 0-4 pts                                       │
│                                                                              │
│ CRITICAL: Award points based on DESCRIPTION alignment, not name similarity  │
│ Names like "ACO_AMT_ACR_RND" vs "accrued_amount" score 35+ if descriptions  │
│ show same functional purpose                                                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ B. TABLE CONTEXT ALIGNMENT (35 points max - FUNCTIONAL FOCUS)               │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Perfect functional table match (same data type and purpose): 30-35 pts    │
│   * Master to Master: Full points                                           │
│   * Fact to Fact: Full points                                               │
│   * NAMES DON'T MATTER - Match by table FUNCTION (master vs transaction)    │
│                                                                              │
│ • Same table archetype across domains: 25-30 pts                            │
│   * Entity Masters: Customer/Party/Client tables (identify entities)        │
│   * Location Masters: Branch/Store/Facility tables (identify places)        │
│   * Product Masters: Item/Account/Policy tables (identify offerings)        │
│   * Transaction Tables: Invoice/Claim/Sale tables (record events)           │
│                                                                              │
│ • Compatible table purposes (e.g., lookup to dimension): 20-25 pts          │
│                                                                              │
│ • PENALTY ONLY FOR FUNDAMENTAL MISMATCHES: -15 pts                          │
│   * Master table PK → Transaction table FK: Apply penalty                   │
│   * Transaction event → Static reference data: Apply penalty                │
│   * Otherwise: NO cross-domain penalty for different naming conventions     │
│                                                                              │
│ CRITICAL: Evaluate table FUNCTION and DATA ROLE, not naming patterns        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ C. BUSINESS LOGIC FITNESS (25 points max)                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Perfect business fit (purpose, granularity, usage align): 20-25 pts       │
│ • Good fit with minor context differences: 15-19 pts                        │
│ • Acceptable fit (functional equivalence exists): 10-14 pts                 │
│ • Column-table coherence penalty: -10 pts if fundamental conflict (not      │
│   naming mismatch)                                                           │
│                                                                              │
│ Consider: Data type compatibility, relationship roles (PK/FK), granularity  │
│ level (account vs transaction level)                                        │
└─────────────────────────────────────────────────────────────────────────────┘

**ADJUSTED CONFIDENCE THRESHOLD:** Minimum 25 points required (lowered for cross-domain flexibility)
**MAXIMUM POSSIBLE SCORE:** 100 points (normalized from 40+35+25)

**CROSS-DOMAIN BONUS**: When source and CDM are from different sectors (banking vs retail vs insurance):
- Add +5 bonus points if functional equivalence is clearly demonstrated in descriptions
- This compensates for inevitable terminology mismatches

**CONFIDENCE THRESHOLDS:**
• 75-100: HIGH CONFIDENCE - Strong match, recommend acceptance
• 50-74:  MEDIUM CONFIDENCE - Good match, human review recommended  
• 30-49:  LOW CONFIDENCE - Possible match, needs careful review
• 25-29:  VERY LOW CONFIDENCE - Marginal match, worth presenting for human review
• <25:    NO MATCH - Return null - insufficient functional alignment

═══════════════════════════════════════════════════════════════════════════════
COMPREHENSIVE REASONING SYNTHESIS
═══════════════════════════════════════════════════════════════════════════════

**CRITICAL**: Each candidate MUST have UNIQUE reasoning explaining its SPECIFIC position
- Candidate 1 (Highest score): Explain why THIS candidate is the BEST match
- Candidate 2 (Second): Explain why it's GOOD but not AS GOOD as Candidate 1
- Candidate 3 (Third): Explain why it's ACCEPTABLE but has MORE LIMITATIONS than Candidates 1 & 2

**Reasoning Must Include:**
- Explicitly reference both app and CDM descriptions for EACH candidate
- Explain how table contexts support or contradict the mapping for EACH candidate
- Consider domain-specific terminology and business context in your evaluation
- Justify decision using multi-level context analysis for EACH candidate
- DO NOT use generic or copy-pasted reasoning - each must reflect the candidate's ranking
- If no candidate meets minimum threshold, return null with detailed reasoning

═══════════════════════════════════════════════════════════════════════════════
CRITICAL SUCCESS FACTORS FOR CROSS-DOMAIN ACCURACY
═══════════════════════════════════════════════════════════════════════════════

1. Read and quote from column descriptions - they reveal functional purpose
2. Identify functional role first (identifier/amount/date/status/description)
3. Compare functional roles across source and CDM - ignore naming differences
4. Award points for PURPOSE alignment, not terminology matching
5. Use table context as supporting evidence, not primary filter
6. When uncertain between naming mismatch vs functional mismatch, trust the descriptions
7. Recognize cross-sector equivalence: customer=policyholder=borrower=shopper (all identify primary party)
8. Match by WHAT data represents, not naming similarity
"""


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PROMPT GENERATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_system_prompt() -> str:
    """
    Get comprehensive system prompt with all frameworks and guidelines.
    Sector-specific context is added dynamically in human prompt.
    """
    return f"""You are a Senior Data Architect with expertise in semantic data modeling, enterprise data integration, and Common Data Model (CDM) design across multiple business sectors including BFSI (Banking, Insurance, Lending, Payments, Wealth Management, Treasury) and Commercial sectors (Retail, Consumer Products).

Your task is to perform precise semantic mapping of application database columns to Common Data Model (CDM) attributes using advanced reasoning, comprehensive context analysis, and sector-aware functional matching.

{FUNDAMENTAL_PRINCIPLES}

{REASONING_FRAMEWORK}

{FUNCTIONAL_ROLE_PATTERNS}

{TABLE_CLASSIFICATION}

{RELATIONSHIP_ANALYSIS}

{ENTITY_IDENTIFICATION}

{CONTEXT_EVALUATION}

{UNIVERSAL_SCORING_FRAMEWORK}

═══════════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT (JSON only)
═══════════════════════════════════════════════════════════════════════════════

Return a JSON array with top candidates (1-3 max, ordered by score):

{{"candidates": [
  {{
    "term": "<CDM column name exactly as provided>",
    "reason": "<UNIQUE reasoning for why THIS is the BEST match. Reference SPECIFIC descriptions and functional role. Explain ranking position.>",
    "score": <integer 25-100>
  }},
  {{
    "term": "<CDM column name>",
    "reason": "<UNIQUE reasoning for why SECOND best. What makes it less suitable than #1? Reference SPECIFIC differences.>",
    "score": <integer, must be less than first>
  }},
  {{
    "term": "<CDM column name>",
    "reason": "<UNIQUE reasoning for why THIRD. What limitations vs #1 and #2? Reference SPECIFIC gaps.>",
    "score": <integer, must be less than second>
  }}
]}}

**CRITICAL CONSTRAINTS:**
- Only include candidates with scores >= 25 (lowered threshold for cross-domain flexibility)
- If all candidates score < 25, return empty array: {{"candidates": []}}
- Order candidates by score (highest first)
- Maximum 3 candidates
- Score composition: Semantic Alignment (40pts) + Table Context (35pts) + Business Fitness (25pts) + Bonuses/Penalties = 100pts max
- **MANDATORY**: Each candidate MUST have DIFFERENT reasoning that explains its SPECIFIC ranking position
- **MANDATORY**: Each reason MUST reference the column descriptions and explain functional alignment
- **PROHIBITED**: DO NOT copy-paste the same reasoning for multiple candidates
- **PROHIBITED**: DO NOT reject candidates solely due to naming differences - evaluate functional purpose
- **REQUIRED**: Each reason must explicitly state the functional role (identifier/amount/date/status/etc.) and how it aligns
- **PREFERENCE**: Aim to return at least 1 candidate if any score >= 25, prioritizing functional equivalence over naming similarity
- Return ONLY valid JSON, no markdown, no additional text
- If uncertain, return empty candidates array rather than forcing a match

Remember: Your reasoning should explicitly reference the provided descriptions and demonstrate functional alignment analysis. Show how you evaluated DATA PURPOSE and FUNCTIONAL ROLE, not just naming similarity. EACH CANDIDATE'S REASONING MUST BE UNIQUE AND POSITION-SPECIFIC, with clear explanation of functional equivalence across sectors.
"""


def get_sector_context(sector: str) -> str:
    """
    Get sector-specific terminology and patterns for contextual awareness.
    
    Args:
        sector: Detected sector name
    
    Returns:
        Formatted string with sector-specific terminology guide
    """
    if sector not in SECTOR_PATTERNS:
        sector = 'banking'  # Fallback
    
    patterns = SECTOR_PATTERNS[sector]
    sector_title = sector.upper()
    
    context = f"""
═══════════════════════════════════════════════════════════════════════════════
SECTOR-SPECIFIC CONTEXTUAL AWARENESS: {sector_title}
═══════════════════════════════════════════════════════════════════════════════

**DETECTED SECTOR:** {sector_title}
You are working with {sector} domain data. Use the terminology and patterns below to 
understand the business context, but ALWAYS prioritize description-based matching.

┌─────────────────────────────────────────────────────────────────────────────┐
│ {sector_title} ENTITY TERMINOLOGY                                                   
├─────────────────────────────────────────────────────────────────────────────┤
"""
    
    # Add entity mappings
    for entity_type, terms in patterns['entities'].items():
        context += f"│ {entity_type.upper()}: {', '.join(terms)}\n"
    
    context += "└─────────────────────────────────────────────────────────────────────────────┘\n\n"
    
    # Add amount terminology
    context += f"""┌─────────────────────────────────────────────────────────────────────────────┐
│ {sector_title} MONETARY AMOUNT PATTERNS                                             
├─────────────────────────────────────────────────────────────────────────────┤
"""
    for amount_type, terms in patterns['amounts'].items():
        context += f"│ {amount_type.upper()}: {', '.join(terms)}\n"
    
    context += "└─────────────────────────────────────────────────────────────────────────────┘\n\n"
    
    # Add date terminology
    context += f"""┌─────────────────────────────────────────────────────────────────────────────┐
│ {sector_title} TEMPORAL FIELD PATTERNS                                              
├─────────────────────────────────────────────────────────────────────────────┤
"""
    for date_type, terms in patterns['dates'].items():
        context += f"│ {date_type.upper()}: {', '.join(terms)}\n"
    
    context += "└─────────────────────────────────────────────────────────────────────────────┘\n\n"
    
    # Add status codes
    context += f"""┌─────────────────────────────────────────────────────────────────────────────┐
│ {sector_title} STATUS/CLASSIFICATION                                                
├─────────────────────────────────────────────────────────────────────────────┤
│ {', '.join(patterns['status'])}
└─────────────────────────────────────────────────────────────────────────────┘

**HOW TO USE THIS CONTEXT:**
1. Recognize that source data may use ANY of these {sector} terms
2. CDM may use DIFFERENT {sector} terminology or generic terms
3. Match based on FUNCTIONAL PURPOSE, not terminology matching
4. If source says "{patterns['entities']['customer'][0]}" and CDM says "customer_id", 
   they may match if descriptions show both identify the primary party
5. Use this context to UNDERSTAND the domain, then MATCH by descriptions

**CROSS-SECTOR TRANSLATION:**
If CDM uses terminology from a DIFFERENT sector, that's OK! Match by function:
- Example: Source is "{patterns['entities']['contract'][0]}" ({sector}) but CDM uses 
  "Agreement ID" (banking term) → Match if both identify the primary contract
"""
    
    return context


def get_human_prompt(app_info: dict, candidates: list) -> str:
    """
    Build human prompt with sector-specific context and candidate details.
    
    Args:
        app_info: Dictionary containing csv_table_name, csv_table_description,
                  csv_column_name, csv_column_description, app_query_text
        candidates: List of candidate dictionaries with term, definition, table, etc.
    
    Returns:
        Formatted human prompt string with sector context
    """
    # Detect sector from context
    detected_sector = detect_sector(app_info)
    sector_context = get_sector_context(detected_sector)
    
    # Format candidates details
    cand_details = []
    for idx, cand in enumerate(candidates[:10], 1):  # Limit to 10 for context window
        details = f"┌── CANDIDATE {idx}: {cand.get('term', 'N/A')}\n"
        details += f"│   CDM Table: {cand.get('table', 'N/A')}\n"
        details += f"│   CDM Table Definition: {cand.get('table_definition', 'N/A')}\n"
        details += f"│   CDM Column Definition: {cand.get('definition', 'N/A')}\n"
        
        # Add entity/hierarchy if available
        if cand.get('entity'):
            details += f"│   Entity/Parent: {cand.get('entity', 'N/A')}\n"
        if cand.get('hierarchy'):
            hierarchy_str = str(cand.get('hierarchy', 'N/A'))[:120]
            if len(str(cand.get('hierarchy', ''))) > 120:
                hierarchy_str += '...'
            details += f"│   Hierarchy: {hierarchy_str}\n"
        
        details += f"└────────────────────────────────────────────────────────────────────────────"
        cand_details.append(details)
    
    cands_str = "\n\n".join(cand_details)
    
    # Build complete human prompt
    human_prompt = f"""
═══════════════════════════════════════════════════════════════════════════════
SOURCE APPLICATION COLUMN (Map this to CDM)
═══════════════════════════════════════════════════════════════════════════════
• Table Name: {app_info.get('csv_table_name', 'N/A')}
• Table Description: {app_info.get('csv_table_description', 'N/A')}
• Column Name: {app_info.get('csv_column_name', 'N/A')}
• Column Description: {app_info.get('csv_column_description', 'N/A')}

Full Context: {app_info.get('app_query_text', 'N/A')}

{sector_context}

═══════════════════════════════════════════════════════════════════════════════
CDM CANDIDATES (Evaluate each using scoring framework)
═══════════════════════════════════════════════════════════════════════════════

{cands_str}

═══════════════════════════════════════════════════════════════════════════════
TASK
═══════════════════════════════════════════════════════════════════════════════

Using the {detected_sector.upper()} sector context above and the COMPREHENSIVE FRAMEWORKS:

1. **Read DESCRIPTIONS first** (source column + CDM columns) - they are primary source of truth
2. **Identify FUNCTIONAL ROLE** of source column from description (identifier/amount/date/status/rate/text)
3. **For EACH candidate**, perform complete evaluation:
   - SEMANTIC: Does description show SAME functional purpose? (0-40pts)
   - TABLE CONTEXT: Do table functions align (master→master, fact→fact)? (0-35pts)
   - BUSINESS FITNESS: Is this a logical business fit with data roles/granularity? (0-25pts)
   - Apply BONUSES: Table type matches (+10), composite key consistency (+5), cross-domain equivalence (+5)
   - Apply PENALTIES: Master→Fact FK mapping (-25), fundamental table mismatch (-15), coherence conflict (-10)
4. **Calculate TOTAL SCORE** for each candidate (max 100 including bonuses/penalties)
5. **Select top 1-3 candidates** with scores >= 25 (adjusted threshold for cross-domain flexibility)
6. **Write UNIQUE, SPECIFIC reasoning** for each, explicitly referencing:
   - Actual column descriptions (quote or paraphrase specific text)
   - Functional role alignment (what both columns represent)
   - Table context support/conflict
   - Why THIS candidate is at THIS ranking position vs others
7. **Order by score** (highest first)

**CRITICAL REMINDERS:**
- Match by FUNCTIONAL PURPOSE from descriptions, NOT by naming similarity
- Cross-sector terms (account/policy/loan, customer/policyholder/shopper) can match if function aligns
- Each reason must be UNIQUE and position-specific - no copy-paste reasoning
- If descriptions show same purpose but names differ dramatically, award HIGH semantic score
- If names match but descriptions show different purposes, award LOW score

Return ONLY valid JSON in the specified format. No markdown, no extra text.
"""
    
    return human_prompt
