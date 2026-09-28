-- Bilan KK quotidien (DKQ only) -- derived from vw_lafa_report_base
-- Fixed per-report filters baked in: source = 'DKQ' and control_code = 'CCH1' (from report spec).
-- Row number N added here so it stays contiguous 1..N (same caveat as Recherche Simplifiée:
--   any SAC-side filter applied after this view will leave gaps in N).
--
-- DP/DA + Nb de jours helpers exposed for the "quotidien"/Type de Date filtering (logic in SAC):
--   DP = date_prelevement (sampled_date), DA = date_mesure (date_result_entered).

CREATE OR REPLACE VIEW prsm_edhid_samplemanager_dw.vw_bilan_kk_dkq AS
SELECT
    ROW_NUMBER() OVER (ORDER BY date_prelevement, sample_number, component_name) AS n,
    control_code,                                                            -- "Code contrôle" (= CCH1)
    sampled_date                                     AS date_et_heure,       -- "Date et heure" (raw dd/mm/yy for display)
    sample_reference,                                                        -- "Echantillon"     (ID_TEXT)
    sample_number,                                                           -- "Num Echantillon" (ID_NUMERIC)
    material_origin                                  AS origine,             -- "Origine"         (LOCATION_ID)
    component_name                                   AS composant,           -- "Composant"
    result_value                                     AS mesures,             -- "Mesures"
    -- ---- Type de Date (DP/DA) + Nb de jours helpers ----
    date_prelevement,                                                        -- DP (real date, for start/end range filter)
    date_mesure,                                                             -- DA (real date, for start/end range filter)
    DATEDIFF(day, date_prelevement, CURRENT_DATE)    AS jours_depuis_dp,
    DATEDIFF(day, date_mesure,      CURRENT_DATE)    AS jours_depuis_da
FROM prsm_edhid_samplemanager_dw.vw_lafa_report_base
WHERE source       = 'DKQ'
  AND control_code = 'CCH1';
