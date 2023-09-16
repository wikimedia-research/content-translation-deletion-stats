def dataframe_to_mediawiki(df, headers=None, footers=None):
    
    def format_multilevel_headers():
        rows = []
        for level in range(df.columns.nlevels):
            headers = []
            i = 0
            while i < len(df.columns):
                col = df.columns[i][level]
                colspan = sum(1 for j in range(i, len(df.columns)) if df.columns[j][level] == col)
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