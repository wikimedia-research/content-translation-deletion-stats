import pandas as pd
import wmfdata as wmf


def generate_quarters(year):
    quarters = {
        'Q1': {'start': (year, 7, 1), 'end': (year, 9, 30)},
        'Q2': {'start': (year, 10, 1), 'end': (year, 12, 31)},
        'Q3': {'start': (year+1, 1, 1), 'end': (year+1, 3, 31)},
        'Q4': {'start': (year+1, 4, 1), 'end': (year+1, 6, 30)}
    }

    for q, dates in quarters.items():
        start_dt = f"{dates['start'][0]}-{dates['start'][1]:02}-{dates['start'][2]:02}"
        end_dt = f"{dates['end'][0]}-{dates['end'][1]:02}-{dates['end'][2]:02}"
        
        snapshot_year, snapshot_month = divmod(dates['end'][1], 12)
        snapshot_year += dates['end'][0]
        mw_snapshot = f"{snapshot_year}-{snapshot_month+1:02}"

        quarters[q] = {
            'start_dt': start_dt,
            'end_dt': end_dt,
            'mw_snapshot': mw_snapshot
        }
    
    return quarters

deletion_stats_query = """
WITH 
    base_counts AS (
        SELECT
            wiki_db,
            
            -- Counting created CX articles
            SUM(CASE 
                    WHEN ARRAY_CONTAINS(revision_tags, 'contenttranslation') THEN 1 
                ELSE 0 
            END) AS created_cx,
            
            -- Counting total created articles
            COUNT(*) AS total_articles,
            
            -- Counting deleted CX articles
            SUM(CASE
                    WHEN ARRAY_CONTAINS(revision_tags, 'contenttranslation')
                     AND revision_is_deleted_by_page_deletion 
                     AND revision_deleted_by_page_deletion_timestamp BETWEEN '{START_DATE}' and '{END_DATE}' THEN 1
                ELSE 0 
             END) AS deleted_cx,
             
            -- Counting total deleted articles
            SUM(CASE 
                    WHEN revision_is_deleted_by_page_deletion 
                     AND revision_deleted_by_page_deletion_timestamp BETWEEN '{START_DATE}' and '{END_DATE}' THEN 1 
                ELSE 0 
            END) AS deleted_articles
        FROM 
            wmf.mediawiki_history mwh
        -- Join canonical data about wikis
        JOIN
            canonical_data.wikis cdw ON mwh.wiki_db = cdw.database_code
        WHERE
            snapshot = '{MW_SNAPSHOT}'
            AND event_timestamp BETWEEN '{START_DATE}' and '{END_DATE}'
            -- Article namespace only
            AND page_namespace = 0
            -- New page creations only
            AND revision_parent_id = 0
            AND event_entity = 'revision'
            AND event_type = 'create'
            -- Remove bots
            AND size(event_user_is_bot_by_historical) <= 0
            -- Limit to Wikipedias only
            AND database_group = 'wikipedia'
            -- Limit to those that are currently live
            AND status = 'open'
        GROUP BY  
            wiki_db
    )

SELECT
    wiki_db,
    created_cx,
    total_articles - created_cx AS created_non_cx,
    deleted_cx,
    deleted_articles - deleted_cx AS deleted_non_cx            
FROM
    base_counts
"""

# function to run a query to get deletion stats of a given quarter
def query_deletion_stats(quarter: str, query=deletion_stats_query):
        
    formatted_query = query.format(
        MW_SNAPSHOT=quarter['mw_snapshot'],
        START_DATE=quarter['start_dt'], 
        END_DATE=quarter['end_dt']
    )
    
    return wmf.spark.run(formatted_query)

# function to calculate overall deletion ratio, and print explantory statements if needed
def overall_deletion_pct(df: pd.DataFrame, filter_threshold=15, period=None, pr=False):
    
    df_filtered = df.query("created_cx > @filter_threshold")
    
    deleted_cx_ratio = round(df_filtered['deleted_cx'].sum() / df_filtered['created_cx'].sum() * 100, 2)
    deleted_non_cx_ratio = round(df_filtered['deleted_non_cx'].sum() / df_filtered['created_non_cx'].sum() * 100, 2)
    
    if pr:
        print(f'During {period}, overall percentage of articles that were deleted,')
        print(f'\t- created using the Content Translation tool: {deleted_cx_ratio}%')
        print(f'\t- created without using the Content Translation Tool: {deleted_non_cx_ratio}%')
    else:
        return {
            'deleted_cx_pct': deleted_cx_ratio,
            'deleted_non_cx_pct': deleted_non_cx_ratio
        }

# calculate deletion (cx & non-cx) ratios by wiki for a given dataframe
def generate_ratios_by_wiki(df: pd.DataFrame, filter_threshold=15):
    df_filtered = df.query("created_cx > @filter_threshold")
    
    df_filtered = df_filtered.assign(
        deleted_cx_pct=round(df_filtered['deleted_cx'] / df_filtered['created_cx'] * 100, 2),
        deleted_non_cx_pct=round(df_filtered['deleted_non_cx'] / df_filtered['created_non_cx'] * 100, 2),
        deletion_pct_diff=lambda x: x['deleted_non_cx_pct'] - x['deleted_cx_pct']
    )
    
    df_filtered = df_filtered.set_index('wiki_db')
    
    return df_filtered