## Data Lineage
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          BRONZE → SILVER → GOLD                             │
│                                                                             │
│  Bronze                    Silver                    Gold                   │
│  ──────                    ──────                    ────                   │
│                                                                             │
│  DIVISIONS ─────────────▶  DIVISIONS ────────────▶  DIM_BRANCH (via joins) │
│  DISTRICTS ─────────────▶  DISTRICTS ────────────▶  DIM_BRANCH (via joins) │
│  THANAS ─────────────────▶  THANAS ──────────────▶  DIM_BRANCH (via joins) │
│  BRANCHES ──────────────▶  BRANCHES ─────────────▶  DIM_BRANCH             │
│                                                                             │
│  CUSTOMER_TYPES ────────▶  CUSTOMER_TYPES ──────▶  DIM_CUSTOMER            │
│  CUSTOMERS ─────────────▶  CUSTOMERS ────────────▶  DIM_CUSTOMER           │
│                                                                             │
│  INDUSTRIES ─────────────▶  INDUSTRIES ──────────▶  DIM_INDUSTRY           │
│  FINANCING_PRODUCTS ────▶  FINANCING_PRODUCTS ──▶  DIM_PRODUCT             │
│                                                                             │
│  BUSINESS_UNITS ────────▶  BUSINESS_UNITS ──────▶  DIM_BUSINESS_UNIT       │
│                                                                             │
│  CL_CATEGORIES ─────────▶  CL_CATEGORIES ──────▶  DIM_CL_CATEGORY          │
│                                                                             │
│  FINANCING_ACCOUNTS ────▶  FINANCING_ACCOUNTS ─▶  FACT_FINANCING_ACCOUNT   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```