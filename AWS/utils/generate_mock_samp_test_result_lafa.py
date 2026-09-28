"""
Generate a mock SAMP_TEST_RESULT_LAFA_prod_extract.csv.gz for both DKQ and FOS.
Values are derived from real lookup data in the existing source files so JOINs work.
"""
import gzip, csv, io, random, os
from datetime import date, timedelta

random.seed(42)

BASE = r'C:\Users\vkarafer\Documents\SampleManager\AWS\sample-manager-source'
OUT_COLS = [
    "ID_NUMERIC","ID_TEXT","CUSTOMER_ID","FOURNISSEUR","PRODUCT","PRODUCT_VERSION",
    "COMMENTAIRES","LOGIN_BY","LOGIN_DATE","SAMPLED_DATE","DUREE_STOCKAGE",
    "CONDITIONNEMENT","NUM_LOT","ON_SPEC","DATE_PREL_VP","LOCATION_ID",
    "DESTINATION_MATIERE","CODE_CONTROLE","SAMPLING_POINT","LOT_AN",
    "ORIGINAL_SAMPLE","TEST_SCHEDULE","JOB_NAME","OLD_STATUS","RECD_DATE",
    "DATE_STARTED","STARTER","DATERESREQ","DATE_COMPLETED","COMPLETER",
    "DATERESAVAIL","DATE_AUTHORISED","AUTHORISER","AUTHORISATION_NOTES",
    "GRADE_CODE","TESTS_TO_DO","BATCH_NAME","SAMPLE_TYPE","SAMPLE_NAME",
    "DESCRIPTION","PREPARATION","INVOICE_NUMBER","TEMPLATE_ID","COMP_PROD_NAME",
    "COMP_PROD_VER","STANDARD","BATCH_ID","AUTO_VALIDATE","HAS_INCIDENTS",
    "PRELEVEUR","SAMPLE_AUTO","INDEX_POLAB","NUM_POLAB","ANALYSE_AUTO","TONNAGE",
    "PLT_NUMBER_FROM","PLT_NUMBER_TO","UNIT_TYPE","PLT_NUMBER","LAF_BATCH",
    "DATE_SHIPPED","PACKAGE","SHIPPING","SHIP_USR","COQ","SUBMIT_SHIP",
    "PRODUIT_QUALIFIE","GRANULOMETRIE","REF_LIVRAISON","DATE_LIVRAISON",
    "CHAMPS_LIBRE1","CHAMPS_LIBRE2","CHAMPS_LIBRE3","CHAMPS_LIBRE4",
    "TEST_NUMBER","TEST_COUNT","REPLICATE_TEST","ANALYSIS","TECHNICIEN",
    "TEST_DATE_STARTED","TEST_DATE_COMPLETED","TEST_OLD_STATUS","TEST_STARTER",
    "TEST_COMPLETER","TEST_DATE_AUTHORISED","TEST_AUTHORISER","COMPONENT_REPLICATES",
    "INSTRUMENT","TEST_ON_SPEC","ORDER_NUM","VALIDATION_STATUS",
    "AUTHORISATION_COMMENT","TEST_AUTO_VALIDATE","TEST_HAS_INCIDENTS",
    "INSTRUMENT_TYPE","ANALYSIS_GROUP","ANALYSIS_VERSION","COMPONENT_NAME",
    "RESULT_TYPE","RESULT_TEXT","RESULT_VALUE","UNITS","MINIMUM","MAXIMUM",
    "OUT_OF_RANGE","DATE_RESULT_ENTERED","RESULT_ENTERED_BY","ORDER_NUMBER",
    "TYPICAL","TRUE_WORD","FALSE_WORD","ALLOWED_CHARACTERS","RESULT_CALCULATION",
    "PLACES","RESULT_OLD_STATUS","RESULT_DATE_AUTHORISED","RESULT_AUTHORISER",
    "RESULT_GROUP_ID","MINIMUM_PQL","MAXIMUM_PQL","LESS_THAN_PQL",
    "GREATER_THAN_PQL","PQL_CALCULATION","FORMULA","RESULT_HAS_INCIDENTS",
    "INSTRUMENT_USED","GROUP_ID","TEST_STATUS","RESULT_STATUS","STATUS","FORMAT"
]

def read_gz(path, key_col, val_cols, limit=20):
    results = []
    with gzip.open(path, 'rt', encoding='windows-1252') as f:
        for row in csv.DictReader(f):
            entry = {c: row[c].strip() for c in [key_col] + val_cols if c in row}
            if entry.get(key_col):
                results.append(entry)
            if len(results) >= limit:
                break
    return results

def oracle_date(d):
    months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    return f"{d.day:02d}-{months[d.month-1]}-{str(d.year)[2:]}"

def rand_date(start_year=2010, end_year=2013):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_result(min_val, max_val):
    try:
        lo, hi = float(min_val), float(max_val)
        if hi == 0:
            hi = 100.0
        val = round(random.uniform(lo, hi * 0.95), 4)
        return str(val)
    except Exception:
        return str(round(random.uniform(0, 100), 4))


def generate(factory):
    dkq_base = os.path.join(BASE, 'DKQ')

    samples    = read_gz(dkq_base + r'\C_SAMPLE_prod_extract.csv.gz',
                         'ID_NUMERIC',
                         ['ID_TEXT','SAMPLED_DATE','SAMPLING_POINT','LOCATION_ID',
                          'CUSTOMER_ID','CODE_CONTROLE','DESTINATION_MATIERE',
                          'JOB_NAME','PRODUCT','PRODUCT_VERSION','SAMPLE_TYPE',
                          'FOURNISSEUR','NUM_LOT','CONDITIONNEMENT','TONNAGE',
                          'GRANULOMETRIE','FORMAT'], limit=15)

    analyses   = read_gz(dkq_base + r'\VERSIONED_ANALYSIS_prod_extract.csv.gz',
                         'IDENTITY', ['ANALYSIS_VERSION'], limit=10)

    components = []
    with gzip.open(dkq_base + r'\VERSIONED_COMPONENT_prod_extract.csv.gz',
                   'rt', encoding='windows-1252') as f:
        for row in csv.DictReader(f):
            n = row['NAME'].strip()
            mn = row['MINIMUM'].strip()
            mx = row['MAXIMUM'].strip()
            u  = row['UNITS'].strip()
            an = row['ANALYSIS'].strip()
            av = row['ANALYSIS_VERSION'].strip()
            if n and mx and mx != '0':
                components.append({'NAME': n, 'MINIMUM': mn, 'MAXIMUM': mx,
                                   'UNITS': u, 'ANALYSIS': an,
                                   'ANALYSIS_VERSION': av})
            if len(components) >= 30:
                break

    jobs = read_gz(dkq_base + r'\JOB_HEADER_prod_extract.csv.gz',
                   'JOB_NAME', ['FOURNISSEUR'], limit=10)

    rows = []
    test_num = 1000

    for smp in samples:
        # pick 2-4 analyses for this sample
        n_analyses = random.randint(2, 4)
        chosen_analyses = random.sample(analyses, min(n_analyses, len(analyses)))

        sampled_dt = rand_date()
        auth_dt    = sampled_dt + timedelta(days=random.randint(1, 5))
        on_spec    = random.choices(['T', 'F'], weights=[80, 20])[0]
        status     = random.choices(['A', 'X'], weights=[90, 10])[0]
        job        = random.choice(jobs)
        technician = random.choice(['MARTIN', 'DUPONT', 'BERNARD', 'LEROY'])

        for ana in chosen_analyses:
            # pick 3-6 components for this analysis
            ana_comps = [c for c in components if c['ANALYSIS'] == ana['IDENTITY']]
            if not ana_comps:
                ana_comps = components[:6]
            chosen_comps = random.sample(ana_comps, min(random.randint(3, 6), len(ana_comps)))

            test_num += 1
            result_status = random.choices(['A', 'U', 'X'], weights=[80, 10, 10])[0]

            for order, comp in enumerate(chosen_comps, 1):
                result_val  = rand_result(comp['MINIMUM'], comp['MAXIMUM'])
                out_of_range = 'F'
                try:
                    lo = float(comp['MINIMUM'])
                    hi = float(comp['MAXIMUM']) if float(comp['MAXIMUM']) != 0 else 100
                    if float(result_val) < lo or float(result_val) > hi:
                        out_of_range = 'T'
                except Exception:
                    pass

                row = {c: '' for c in OUT_COLS}
                # sample columns
                row['ID_NUMERIC']           = smp.get('ID_NUMERIC','')
                row['ID_TEXT']              = smp.get('ID_TEXT','')
                row['CUSTOMER_ID']          = smp.get('CUSTOMER_ID','')
                row['FOURNISSEUR']          = smp.get('FOURNISSEUR', job.get('FOURNISSEUR',''))
                row['PRODUCT']              = smp.get('PRODUCT','')
                row['PRODUCT_VERSION']      = smp.get('PRODUCT_VERSION','1')
                row['LOGIN_BY']             = technician
                row['LOGIN_DATE']           = oracle_date(sampled_dt)
                row['SAMPLED_DATE']         = oracle_date(sampled_dt)
                row['ON_SPEC']              = on_spec
                row['LOCATION_ID']          = smp.get('LOCATION_ID','')
                row['DESTINATION_MATIERE']  = smp.get('DESTINATION_MATIERE','')
                row['CODE_CONTROLE']        = smp.get('CODE_CONTROLE','')
                row['SAMPLING_POINT']       = smp.get('SAMPLING_POINT','')
                row['ORIGINAL_SAMPLE']      = smp.get('ID_NUMERIC','')
                row['JOB_NAME']             = smp.get('JOB_NAME', job.get('JOB_NAME',''))
                row['OLD_STATUS']           = 'A'
                row['RECD_DATE']            = oracle_date(sampled_dt)
                row['DATE_STARTED']         = oracle_date(sampled_dt)
                row['DATE_COMPLETED']       = oracle_date(auth_dt)
                row['DATE_AUTHORISED']      = oracle_date(auth_dt)
                row['AUTHORISER']           = technician
                row['SAMPLE_TYPE']          = smp.get('SAMPLE_TYPE','STANDARD')
                row['NUM_LOT']              = smp.get('NUM_LOT','')
                row['CONDITIONNEMENT']      = smp.get('CONDITIONNEMENT','')
                row['TONNAGE']              = smp.get('TONNAGE','')
                row['GRANULOMETRIE']        = smp.get('GRANULOMETRIE','')
                row['FORMAT']               = smp.get('FORMAT','')
                row['AUTO_VALIDATE']        = 'F'
                row['HAS_INCIDENTS']        = 'F'
                row['STATUS']               = status
                # test columns
                row['TEST_NUMBER']          = str(test_num)
                row['TEST_COUNT']           = '1'
                row['REPLICATE_TEST']       = 'F'
                row['ANALYSIS']             = ana['IDENTITY']
                row['TECHNICIEN']           = technician
                row['TEST_DATE_STARTED']    = oracle_date(sampled_dt)
                row['TEST_DATE_COMPLETED']  = oracle_date(auth_dt)
                row['TEST_OLD_STATUS']      = 'A'
                row['TEST_STARTER']         = technician
                row['TEST_COMPLETER']       = technician
                row['TEST_DATE_AUTHORISED'] = oracle_date(auth_dt)
                row['TEST_AUTHORISER']      = technician
                row['COMPONENT_REPLICATES'] = '1'
                row['INSTRUMENT']           = random.choice(['XRF-01','LECO-01','ICP-01','ANAL-01'])
                row['TEST_ON_SPEC']         = on_spec
                row['ORDER_NUM']            = str(order)
                row['VALIDATION_STATUS']    = 'A'
                row['TEST_AUTO_VALIDATE']   = 'F'
                row['TEST_HAS_INCIDENTS']   = 'F'
                row['ANALYSIS_VERSION']     = ana.get('ANALYSIS_VERSION','1')
                row['ANALYSIS_GROUP']       = 'CHIMIE'
                # result columns
                row['COMPONENT_NAME']       = comp['NAME']
                row['RESULT_TYPE']          = 'N'
                row['RESULT_TEXT']          = ' '
                row['RESULT_VALUE']         = result_val
                row['UNITS']                = comp['UNITS']
                row['MINIMUM']              = comp['MINIMUM']
                row['MAXIMUM']              = comp['MAXIMUM']
                row['OUT_OF_RANGE']         = out_of_range
                row['DATE_RESULT_ENTERED']  = oracle_date(auth_dt)
                row['RESULT_ENTERED_BY']    = technician
                row['ORDER_NUMBER']         = str(order)
                row['PLACES']               = '4'
                row['RESULT_OLD_STATUS']    = 'A'
                row['RESULT_DATE_AUTHORISED'] = oracle_date(auth_dt)
                row['RESULT_AUTHORISER']    = technician
                row['TEST_STATUS']          = status
                row['RESULT_STATUS']        = result_status

                rows.append(row)

    return rows


for factory in ['DKQ', 'FOS']:
    out_path = os.path.join(BASE, factory, 'SAMP_TEST_RESULT_LAFA_prod_extract.csv.gz')
    rows = generate(factory)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=OUT_COLS, quoting=csv.QUOTE_ALL, lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode('windows-1252')
    with gzip.open(out_path, 'wb') as f:
        f.write(csv_bytes)
    print(f'{factory}: wrote {len(rows)} rows -> {out_path}')
