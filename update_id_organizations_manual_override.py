import os
import pandas as pd
from datetime import datetime
from we_module import services
import json
import clearbit
clearbit.key = os.environ['CLEARBIT_KEY']
COMPANY_TABLE_NAME = 'mv_enriched_companies_historic'


def config():
    return {
        'email_me': None,
        'text_me': '15082571861',
        'schedule': [{
            'type': 'normal',
            'table_name': 'mv_manual_company_organization_overrides',
            'enabled': True,
            'start_at': '2016-01-01',
            'timezone': 'America/New_York',
            'cron_schedule': '0 3 * * *',
            'input_json': {},
            'incremental_field': None,
            'keys': {
                'primarykey': None,
                'sortkey': None,
                'distkey': None
            }
        }]
    }


def construct_company_dict(company, time_now):
    if not company:
        return None
    formatted_company = {
        'company_id': company['id'],
        'name': company['name'],
        'legal_name': company['legalName'],
        'domain': company['domain'],
        'domain_aliases': company['domainAliases'],
        'site_title': company['site']['title'],
        'site_h1': company['site']['h1'],
        'site_meta_description': company['site']['metaDescription'],
        'site_meta_author': company['site']['metaAuthor'],
        'site_phone_numbers': company['site']['phoneNumbers'],
        'site_email_addresses': company['site']['emailAddresses'],
        'sector': company['category']['sector'],
        'industry_group': company['category']['industryGroup'],
        'industry': company['category']['industry'],
        'sub_industry': company['category']['subIndustry'],
        'tags': company['tags'],
        'description': company['description'],
        'founded_year': company['foundedYear'],
        'location': company['location'],
        'time_zone': company['timeZone'],
        'utc_offset': company['utcOffset'],
        'street_number': company['geo']['streetNumber'],
        'street_name': company['geo']['streetName'],
        'sub_premise': company['geo']['subPremise'],
        'city': company['geo']['city'],
        'postal_code': company['geo']['postalCode'],
        'state': company['geo']['state'],
        'state_code': company['geo']['stateCode'],
        'country': company['geo']['country'],
        'country_code': company['geo']['countryCode'],
        'latitude': company['geo']['lat'],
        'longitude': company['geo']['lng'],
        'logo': company['logo'],
        'facebook_handle': company['facebook']['handle'],
        'linkedin_handle': company['linkedin']['handle'],
        'twitter_handle': company['twitter']['handle'],
        'twitter_id': company['twitter']['id'],
        'twitter_bio': company['twitter']['bio'],
        'twitter_followers': company['twitter']['followers'],
        'twitter_following': company['twitter']['following'],
        'twitter_location': company['twitter']['location'],
        'twitter_site': company['twitter']['site'],
        'twitter_avatar': company['twitter']['avatar'],
        'crunchbase_handle': company['crunchbase']['handle'],
        'email_provider': company['emailProvider'],
        'type': company['type'],
        'ticker': company['ticker'],
        'phone': company['phone'],
        'indexed_at': company['indexedAt'],
        'alexa_us_rank': company['metrics']['alexaUsRank'],
        'alexa_global_rank': company['metrics']['alexaGlobalRank'],
        'employees': company['metrics']['employees'],
        'employee_range': company['metrics']['employeesRange'],
        'market_cap': company['metrics']['marketCap'],
        'raised': company['metrics']['raised'],
        'annual_revenue': company['metrics']['annualRevenue'],
        'tech': None,
        '_run_at': time_now
    }
    return formatted_company


def format_override_entry(company, profile):
    formatted_override = {
        'wework_company_uuid': company['uuid'],
        'organization_uuid': profile['company_id'],
        'parent_domain': company['parent_domain'],
        'decided_by': company['decided_by']
    }
    return formatted_override


def send_email_about_possible_mistake(account_name, account_uuid, company_data):
    send_to = 'tom.bescherer@wework.com'
    # send_to = 'rdelano@wework.com'
    reply_to = 'tom.bescherer@wework.com'
    email_subject = '[Data Alert]: Possible False Positive for Organization Override'
    email_str = '''
        Hello!
        I noticed that the account {name} (uuid : {account_uuid}) that you tried to add did not have 500 employees. 
        This could be because the domain you entered was incorrect. 
        Clearbit returned the following:
        {company_data}
        Please update the domain (it will run again tomorrow morning) or reply to this email.  
        Thanks
        Taylor & Tom
    '''.format(
            name=account_name,
            account_uuid=account_uuid,
            company_data=json.dumps(company_data, indent=4, sort_keys=True)
        )

    services.send_email(
        send_to,
        email_subject,
        message=email_str,
        reply_to=reply_to
    )

    return None


def main(we, **kwargs):

    sheet_key = '1mATVSuoFudkvhtcJxjna-ifNrpB88o85yqH4mHoJB7o'
    sheet_name = 'organization_creation_override'
    df_gsheet = we.get_google_sheet(sheet_key, sheet_name)

    df_redshift = we.get_tbl_query('''
        SELECT
            "decided_by",
            "organization_uuid",
            "parent_domain",
            "wework_company_uuid"
        FROM dw.mv_manual_company_organization_overrides
    ''')
    common = df_gsheet.merge(df_redshift, left_on='uuid', right_on='wework_company_uuid')

    new_overrides = df_gsheet[(~df_gsheet.uuid.isin(common.uuid))].reset_index(drop=True)
    add_list = []
    override_entries = []
    time_now = datetime.now()
    for ii, row in new_overrides.iterrows():
        company = clearbit.Company.find(domain=row['parent_domain'], stream=True)
        if company is not None and 'metrics' in company.keys() and isinstance(company['metrics'], dict) and 'employees' in company['metrics'].keys():
            profile = construct_company_dict(company, time_now)
            override_entry = format_override_entry(row, profile)
            add_list.append(profile)
            override_entries.append(override_entry)
        else:
            send_email_about_possible_mistake(row['name'], row['uuid'], company)

    # add newly discovered orgs to clearbit companies table
    new_companies = pd.DataFrame(add_list)
    we.append_table(
        new_companies,
        'dw',
        COMPANY_TABLE_NAME
    )
    # save companies override to override table, including new rows
    return pd.concat([df_redshift, pd.DataFrame(override_entries)], ignore_index=True)