-- Recherche Simplifiée (FOS + DKQ) -- derived from vw_lafa_report_base
-- One unified view for both factories; SAC filters on `source` (DKQ / FOS).
-- Row number N is added here (per-report wrapper) so it stays contiguous 1..N per factory.
-- NOTE: any SAC-side filter applied AFTER this view will leave gaps in N. If the report
--       needs a gap-free N after runtime filtering, do the numbering in SAC instead.
--
-- "Type de Date" (DP/DA) + "Nb de jours" enhancement:
--   DP = date_prelevement (sampled_date), DA = date_mesure (date_result_entered).
--   The DP/DA selection, and "Nb de jours supersedes start/end", live in SAC (a view is not
--   parameterized). This view just exposes both dates + day-count helpers so SAC can filter:
--     DP:  date_prelevement BETWEEN :start AND :end   |  jours_depuis_dp BETWEEN 0 AND :n
--     DA:  date_mesure      BETWEEN :start AND :end   |  jours_depuis_da BETWEEN 0 AND :n
--   jours_depuis_* uses CURRENT_DATE - date, so n=3 keeps today + previous 3 days (0..3 inclusive).

CREATE OR REPLACE VIEW prsm_edhid_samplemanager_dw.vw_recherche_simplifiee AS
SELECT
    ROW_NUMBER() OVER (PARTITION BY source
                       ORDER BY date_prelevement, sample_number, component_name) AS n,
    source,
    sampled_date                                     AS date_et_heure,      -- "Date et heure" (raw dd/mm/yy for display)
    sample_reference,                                                        -- "Echantillon"      (ID_TEXT)
    sample_number,                                                           -- "Num Echantillon"  (ID_NUMERIC)
    sample_type                                      AS type_echantillon,   -- "Type d'échantillon"
    material_origin                                  AS origine,            -- "Origine"          (LOCATION_ID)
    destination_material                             AS destination,        -- "Destination"      (DESTINATION_MATIERE)
    commentaires                                     AS comments,           -- "Commentaire"
    component_name                                   AS composant,          -- "Composant"
    result_value                                     AS mesures,            -- "Measures"
    -- ---- Type de Date (DP/DA) + Nb de jours helpers ----
    date_prelevement,                                                       -- DP (real date, for start/end range filter)
    date_mesure,                                                            -- DA (real date, for start/end range filter)
    DATEDIFF(day, date_prelevement, CURRENT_DATE)    AS jours_depuis_dp,
    DATEDIFF(day, date_mesure,      CURRENT_DATE)    AS jours_depuis_da
FROM prsm_edhid_samplemanager_dw.vw_lafa_report_base;
