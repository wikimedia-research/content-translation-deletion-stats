def dataframe_to_mediawiki(df, headers=None, footers=None):
    
    table = []
    table.append("{| class='wikitable'")
    
    if headers:
        for header in headers:
            table.append(f"! colspan='{len(df.columns)}' | {header}")
            table.append("|-")
    
    for level in range(df.columns.nlevels):
        table_row = "!"
        i = 0
        while i < len(df.columns):
            col = df.columns[i][level]
            colspan = 1
            
            for j in range(i + 1, len(df.columns)):
                if df.columns[j][level] == col:
                    colspan += 1
                else:
                    break
            
            if colspan > 1:
                table_row += f" colspan='{colspan}' | {col} !!"
                i += colspan
            else:
                table_row += f"{col} !!"
                i += 1
        
        table_row = table_row.rstrip(" !!")
        table.append(table_row)
        table.append("|-")
    
    for index, row in df.iterrows():
        table_row = "|"
        for cell in row:
            table_row += f" {cell} ||"
        table_row = table_row.rstrip("||")
        table.append(table_row)
        table.append("|-")
    
    if footers:
        for footer in footers:
            table.append(f"| style=\"text-align:left;\" colspan='{len(df.columns)}' | {footer}")
            table.append("|-")
    
    table.append("|}")

    return "\n".join(table)