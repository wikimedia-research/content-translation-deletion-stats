# formats a pandas Series into percentage
# for example, 0.23 gets formatted to 23%
def format_percent(column, df):
    return df[column].map('{:.2%}'.format)

# coverts a pandas DataFrame to MediaWiki table markup
def dataframe_to_mediawiki(df, headers=None, footers=None):
    
    def format_multilevel_headers():
        rows = []
        for level in range(df.columns.nlevels):
            headers = []
            i = 0
            while i < len(df.columns):
                col = df.columns[i][level] if isinstance(df.columns[i], tuple) else df.columns[i]
                colspan = sum(1 for j in range(i, len(df.columns)) if (df.columns[j][level] if isinstance(df.columns[j], tuple) else df.columns[j]) == col)
                headers.append(f"colspan='{colspan}' | {col}")
                i += colspan
            rows.append(" !! ".join(headers))
        return rows
    
    table = ["{| class='wikitable'"]
    
    # headers
    if headers:
        table.extend([f"! colspan='{len(df.columns)}' | {header}" for header in headers])
        table.append("|-")
    
    table.extend(format_multilevel_headers())
    table.append("|-")
    
    # rows
    for _, row in df.iterrows():
        table.append("| " + " || ".join(map(str, row)))
        table.append("|-")
    
    # footers
    if footers:
        table.extend([f"| style=\"text-align:left;\" colspan='{len(df.columns)}' | {footer}" for footer in footers])
        table.append("|-")
    
    table.append("|}")

    return "\n".join(table)

# add footnote (as superscript) for items present in a given list
def add_footnote(wiki, wiki_list, note_level=2):
    if wiki in wiki_list:
        return f'{wiki}<sup>{note_level}</sup>'
    else:
        return wiki