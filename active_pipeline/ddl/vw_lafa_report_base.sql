CREATE OR REPLACE VIEW prsm_edhid_samplemanager_dw.vw_lafa_report_base AS
select
    row_number() over (partition by source
                       order by date_prelevement, sample_number, component_name) as n,
    u.*
from (
select
st.source,
st.source as site,
st.id_numeric as sample_number,
st.id_text as sample_reference,
to_char(case when split_part(st.sampled_date,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(st.sampled_date,' ',1),6)||case when right(split_part(st.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.sampled_date,' ',1),2),'DD/MM/YYYY')
             when split_part(st.sampled_date,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(st.sampled_date,' ',1),7)||case when right(split_part(st.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.sampled_date,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY') as sampled_date,   -- DP / "Date et heure"
to_char(case when split_part(st.login_date,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(st.login_date,' ',1),6)||case when right(split_part(st.login_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.login_date,' ',1),2),'DD/MM/YYYY')
             when split_part(st.login_date,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(st.login_date,' ',1),7)||case when right(split_part(st.login_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.login_date,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY') as analysis_date,   -- "Analyse" (login_date)
to_char(case when split_part(st.date_result_entered,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(st.date_result_entered,' ',1),6)||case when right(split_part(st.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.date_result_entered,' ',1),2),'DD/MM/YYYY')
             when split_part(st.date_result_entered,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(st.date_result_entered,' ',1),7)||case when right(split_part(st.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.date_result_entered,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY') as measure_date,   -- DA (date_result_entered)
case when split_part(st.sampled_date,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(st.sampled_date,' ',1),6)||case when right(split_part(st.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.sampled_date,' ',1),2),'DD/MM/YYYY')
     when split_part(st.sampled_date,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(st.sampled_date,' ',1),7)||case when right(split_part(st.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.sampled_date,' ',1),2),'DD-MON-YYYY') end as date_prelevement,   -- DP real date
case when split_part(st.date_result_entered,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(st.date_result_entered,' ',1),6)||case when right(split_part(st.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.date_result_entered,' ',1),2),'DD/MM/YYYY')
     when split_part(st.date_result_entered,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(st.date_result_entered,' ',1),7)||case when right(split_part(st.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(st.date_result_entered,' ',1),2),'DD-MON-YYYY') end as date_mesure,   -- DA real date
st.sample_type,
st.customer_id as customer_code,
st.location_id as material_origin,
st.sampling_point,
st.preleveur as sampler,
st.destination_matiere AS destination_material,
st.tonnage,
st.conditionnement as packaging,
st.num_lot as lot_number,
st.ref_livraison as delivery_note,
st.commentaires,
st.product as product_code,
st.produit_qualifie as qualified_product,
case when h.removeflag='F' then 'OUI' else 'NON' end as product_active,
coalesce(h.description, 'Bauxite Rouge ( 40-100 )') as product_label,   -- MOCK fallback until mlp_header product key is fixed
st.code_controle AS control_code,
case when c.removeflag='F' then 'OUI' else 'NON' end as control_code_active,
st.analysis AS analysis_code,
case when trim(va.analysis_type)='' or va.analysis_type is null then 'USINE' else va.analysis_type end as analysis_type,
st.component_name,
st.result_value,
'ECART_MP' as spec_level,     -- PLACEHOLDER value; real source mlp_view.level_id (not ingested)
'52.5' as target_value,       -- PLACEHOLDER value; real source mlp_view.typical_text (not ingested)
'54.50' as spec_min,          -- PLACEHOLDER value; real source mlp_view.min_limit (not ingested)
'100.00' as spec_max,         -- PLACEHOLDER value; real source mlp_view.max_limit (not ingested)
case when st.out_of_range='T' then 'NON' else 'OUI' end as auto_qualification,
case when st.on_spec='T' then 'OUI' else 'NON' end as sample_qualification,
p.phrase_text as granulometry,
'CONCASSE' as shape,          -- PLACEHOLDER value; real source phrase_format.phrase_text (not ingested)
j.job_name,
j.fournisseur as supplier,
coalesce(to_char(case when split_part(j.date_livraison,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(j.date_livraison,' ',1),6)||case when right(split_part(j.date_livraison,' ',1),2)<='50' then '20' else '19' end||right(split_part(j.date_livraison,' ',1),2),'DD/MM/YYYY') when split_part(j.date_livraison,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(j.date_livraison,' ',1),7)||case when right(split_part(j.date_livraison,' ',1),2)<='50' then '20' else '19' end||right(split_part(j.date_livraison,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY'), j.date_livraison) as delivery_date,   -- normalized dd/mm/yy if recognized; else original
j.ref_livraison as delivery_reference,
j.lot_fournisseur as supplier_lot_number,
j.tas_stockage as storage_pile

from prsm_edhid_samplemanager_dw.samp_test_result_lafa st
LEFT JOIN prsm_edhid_samplemanager_dw.code_controle c ON st.code_controle:: text = c."identity":: text
left join prsm_edhid_samplemanager_dw.mlp_header h on h."identity"=st.product and h.product_version=st.product_version
left join prsm_edhid_samplemanager_dw.job_header j on st.job_name:: text = j.job_name:: text
left join prsm_edhid_samplemanager_dw.versioned_analysis va on va."identity":: text = st.analysis:: text and va.analysis_version = st.analysis_version
left join prsm_edhid_samplemanager_dw.phrase p on p.phrase_id:: text = st.granulometrie:: text and p.phrase_type='GRANULO' and p.source = st.source
-- left join prsm_edhid_samplemanager_dw.phrase_format pf on pf.phrase_id:: text = st."format":: text and pf.source = st.source   -- TODO: re-enable when phrase_format is ingested
-- left join prsm_edhid_samplemanager_dw.mlp_view mv on mv.analysis_id:: text = st.analysis:: text and mv.component_name:: text = st.component_name:: text and mv.product_id:: text = st.product:: text and mv.product_version = st.product_version   -- TODO: re-enable when mlp_view is ingested

UNION

select
ct.source,
ct.source as site,
ct.id_numeric as sample_number,
ct.id_text as sample_reference,
to_char(case when split_part(ct.sampled_date,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(ct.sampled_date,' ',1),6)||case when right(split_part(ct.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.sampled_date,' ',1),2),'DD/MM/YYYY')
             when split_part(ct.sampled_date,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(ct.sampled_date,' ',1),7)||case when right(split_part(ct.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.sampled_date,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY') as sampled_date,   -- DP / "Date et heure"
to_char(case when split_part(ct.login_date,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(ct.login_date,' ',1),6)||case when right(split_part(ct.login_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.login_date,' ',1),2),'DD/MM/YYYY')
             when split_part(ct.login_date,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(ct.login_date,' ',1),7)||case when right(split_part(ct.login_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.login_date,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY') as analysis_date,   -- "Analyse" (login_date)
to_char(case when split_part(ct.date_result_entered,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(ct.date_result_entered,' ',1),6)||case when right(split_part(ct.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.date_result_entered,' ',1),2),'DD/MM/YYYY')
             when split_part(ct.date_result_entered,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(ct.date_result_entered,' ',1),7)||case when right(split_part(ct.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.date_result_entered,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY') as measure_date,   -- DA (date_result_entered)
case when split_part(ct.sampled_date,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(ct.sampled_date,' ',1),6)||case when right(split_part(ct.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.sampled_date,' ',1),2),'DD/MM/YYYY')
     when split_part(ct.sampled_date,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(ct.sampled_date,' ',1),7)||case when right(split_part(ct.sampled_date,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.sampled_date,' ',1),2),'DD-MON-YYYY') end as date_prelevement,   -- DP real date
case when split_part(ct.date_result_entered,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(ct.date_result_entered,' ',1),6)||case when right(split_part(ct.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.date_result_entered,' ',1),2),'DD/MM/YYYY')
     when split_part(ct.date_result_entered,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(ct.date_result_entered,' ',1),7)||case when right(split_part(ct.date_result_entered,' ',1),2)<='50' then '20' else '19' end||right(split_part(ct.date_result_entered,' ',1),2),'DD-MON-YYYY') end as date_mesure,   -- DA real date
ct.sample_type,
ct.customer_id as customer_code,
ct.location_id as material_origin,
ct.sampling_point,
ct.preleveur as sampler,
ct.destination_matiere AS destination_material,
ct.tonnage,
ct.conditionnement as packaging,
ct.num_lot as lot_number,
ct.ref_livraison as delivery_note,
ct.commentaires,
ct.product as product_code,
ct.produit_qualifie as qualified_product,
case when h.removeflag='F' then 'OUI' else 'NON' end as product_active,
coalesce(h.description, 'Bauxite Rouge ( 40-100 )') as product_label,   -- MOCK fallback until mlp_header product key is fixed
ct.code_controle AS control_code,
case when c.removeflag='F' then 'OUI' else 'NON' end as control_code_active,
ct.analysis AS analysis_code,
case when trim(va.analysis_type)='' or va.analysis_type is null then 'USINE' else va.analysis_type end as analysis_type,
ct.component_name,
ct.result_value,
'ECART_MP' as spec_level,     -- PLACEHOLDER value; real source mlp_view.level_id (not ingested)
'52.5' as target_value,       -- PLACEHOLDER value; real source mlp_view.typical_text (not ingested)
'54.50' as spec_min,          -- PLACEHOLDER value; real source mlp_view.min_limit (not ingested)
'100.00' as spec_max,         -- PLACEHOLDER value; real source mlp_view.max_limit (not ingested)
case when ct.out_of_range='T' then 'NON' else 'OUI' end as auto_qualification,
case when ct.on_spec='T' then 'OUI' else 'NON' end as sample_qualification,
p.phrase_text as granulometry,
'CONCASSE' as shape,          -- PLACEHOLDER value; real source phrase_format.phrase_text (not ingested)
j.job_name,
j.fournisseur as supplier,
coalesce(to_char(case when split_part(j.date_livraison,' ',1) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{2}$' then to_date(left(split_part(j.date_livraison,' ',1),6)||case when right(split_part(j.date_livraison,' ',1),2)<='50' then '20' else '19' end||right(split_part(j.date_livraison,' ',1),2),'DD/MM/YYYY') when split_part(j.date_livraison,' ',1) ~ '^[0-9]{2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[0-9]{2}$' then to_date(left(split_part(j.date_livraison,' ',1),7)||case when right(split_part(j.date_livraison,' ',1),2)<='50' then '20' else '19' end||right(split_part(j.date_livraison,' ',1),2),'DD-MON-YYYY') end,'DD/MM/YYYY'), j.date_livraison) as delivery_date,   -- normalized dd/mm/yy if recognized; else original
j.ref_livraison as delivery_reference,
j.lot_fournisseur as supplier_lot_number,
j.tas_stockage as storage_pile

from prsm_edhid_samplemanager_dw.c_samp_test_result_lafa ct
LEFT JOIN prsm_edhid_samplemanager_dw.code_controle c ON ct.code_controle:: text = c."identity":: text
left join prsm_edhid_samplemanager_dw.mlp_header h on h."identity"=ct.product and h.product_version=ct.product_version
left join prsm_edhid_samplemanager_dw.job_header j on ct.job_name:: text = j.job_name:: text
left join prsm_edhid_samplemanager_dw.versioned_analysis va on va."identity":: text = ct.analysis:: text and va.analysis_version = ct.analysis_version
left join prsm_edhid_samplemanager_dw.phrase p on p.phrase_id:: text = ct.granulometrie:: text and p.phrase_type='GRANULO' and p.source = ct.source
-- left join prsm_edhid_samplemanager_dw.phrase_format pf on pf.phrase_id:: text = ct."format":: text and pf.source = ct.source   -- TODO: re-enable when phrase_format is ingested
-- left join prsm_edhid_samplemanager_dw.mlp_view mv on mv.analysis_id:: text = ct.analysis:: text and mv.component_name:: text = ct.component_name:: text and mv.product_id:: text = ct.product:: text and mv.product_version = ct.product_version   -- TODO: re-enable when mlp_view is ingested
) u;
