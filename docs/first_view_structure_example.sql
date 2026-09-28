select
st.source,
st.code_controle AS control_code,
case when c.removeflag='F' then 'OUI' else 'NON' end as control_code_active,
st.product as product_code,
case when h.removeflag='F' then 'OUI' else 'NON' end as product_active,
st.analysis AS analysis_code,
st.component_name,
st.id_numeric as sample_number,
st.customer_id as customer_code,
st.location_id as material_origin,
st.tonnage,
st.sample_type,
st.conditionnement as packaging,
st.num_lot as lot_number,
st.ref_livraison as delivery_note,
'04/09/2012 09:20:00'  as date_et_heure,
st.id_text as sample_reference,
st.destination_matiere AS destination_material,
st.commentaires,
st.result_value

from prsm_edhid_samplemanager_dw.samp_test_result_lafa st
LEFT JOIN prsm_edhid_samplemanager_dw.code_controle c ON st.code_controle:: text = c."identity":: text
left join prsm_edhid_samplemanager_dw.mlp_header h on h."identity"=st.product and h.product_version=st.product_version

UNION

select
ct.source,
ct.code_controle AS control_code,
case when c.removeflag='F' then 'OUI' else 'NON' end as control_code_active,
ct.product as product_code,
case when h.removeflag='F' then 'OUI' else 'NON' end as product_active,
ct.analysis AS analysis_code,
ct.component_name,
ct.id_numeric as sample_number,
ct.customer_id as customer_code,
ct.location_id as material_origin,
ct.tonnage,
ct.sample_type,
ct.conditionnement as packaging,
ct.num_lot as lot_number,
ct.ref_livraison as delivery_note,
'04/09/2012 09:20:00' as date_et_heure,
ct.id_text as sample_reference,
ct.destination_matiere AS destination_material,
ct.commentaires,
ct.result_value

from prsm_edhid_samplemanager_dw.c_samp_test_result_lafa ct
LEFT JOIN prsm_edhid_samplemanager_dw.code_controle c ON ct.code_controle:: text = c."identity":: text
left join prsm_edhid_samplemanager_dw.mlp_header h on h."identity"=ct.product and h.product_version=ct.product_version