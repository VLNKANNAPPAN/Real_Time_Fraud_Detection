# Phase 02 — deterministic synthetic data

## Concept

Fraud labels are sampled from a probability influenced by multiple imperfect signals: amount relative to a hidden user profile, alternative device, country, time of day, merchant category, account age, payment method, recent velocity, and interactions. Gaussian noise is added before Bernoulli sampling. This produces legitimate suspicious events and some fraud among ordinary-looking events. There is no `is_fraud` rule embedded in the event payload.

`generate_dataset(seed, n_users, n_transactions)` uses a single NumPy random generator. It emits `users`, immutable v1 `events`, and `labels` as separate tables. Each transaction has an event time and a later producer time. Sorting by producer time creates the simulated arrival order; a small tail of longer delays creates out-of-order events. The label file includes `label_available_at` one or two days later; this timestamp controls chronological training eligibility and is excluded from model inputs. Amounts are positive INR paise. `fraud-generate` writes CSVs and a seed/version manifest under ignored `data/generated/`.

Core label logic (abridged from `simulator.py`):

```python
logit = (
    -4.7
    + 0.9 * high_amount
    + 0.65 * new_device
    + 0.8 * (country != "IN")
    + 0.45 * (velocity >= 3)
    + 0.75 * (high_amount and new_device)
    + noise
)
fraud_label = int(rng.random() < 1 / (1 + np.exp(-logit)))
```

The actual code includes the other factors listed above. `high_amount`, hidden profile amount, sampled probability, and noise are not copied to the event table. `account_age_days` is an as-of-event attribute, not the user's age at dataset export.

## Leakage audit and tests

Tests compare complete generated tables under the same seed, check positive amounts and valid timestamp order, assert the label is absent from user/event tables, check class imbalance, and verify both ordinary-looking fraud and suspicious legitimate records exist. This is a **synthetic** exercise; its class prevalence and correlations cannot validate a deployed fraud model. A source of residual mismatch is that label generation sees a hidden lifetime user amount profile while `features-v1` sees only prior 24-hour history. That difference is intentional realism but makes the baseline harder.
