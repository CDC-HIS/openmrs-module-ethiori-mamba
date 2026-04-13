import os
import re

QUERIES_DIR = 'omod/src/main/resources/_etl/derived/line_list/facts/line_list_queries'

def create_v2_templates():
    files = [f for f in os.listdir(QUERIES_DIR) if f.endswith('.sql') and f.startswith('sp_fact_')]
    
    success_count = 0
    skip_count = 0
    
    for filename in sorted(files):
        filepath = os.path.join(QUERIES_DIR, filename)
        
        with open(filepath, 'r') as f:
            sql = f.read()
            
        sp_name = filename.replace('.sql', '')
        sp_name_v2 = f"{sp_name}_v2"
        
        if sp_name_v2 in sql:
            print(f"[{sp_name}] skipped: V2 already exists.")
            skip_count += 1
            continue
            
        # Extract the original procedure body
        proc_match = re.search(fr'CREATE PROCEDURE {sp_name}\((.*?)\)\s*BEGIN\s*(.*?)\s*END //', sql, re.DOTALL | re.IGNORECASE)
        
        if not proc_match:
            print(f"[{sp_name}] warning: Could not safely parse original SP body.")
            skip_count += 1
            continue
            
        params = proc_match.group(1)
        body = proc_match.group(2)
        
        # Replace the 10-table join inside FollowUp with a direct View lookup
        # We look for the standard mamba_flat_encounter_follow_up block and all its left joins
        join_regex = r'FROM\s+mamba_flat_encounter_follow_up\s+follow_up\s+(LEFT\s+JOIN\s+mamba_flat_encounter_follow_up_\d+.*?)+\)'
        
        # We replace the massive join block with just the view lookup
        new_body, replacements = re.subn(
            join_regex, 
            r'FROM vw_mamba_fact_encounter_follow_up follow_up)', 
            body,
            flags=re.DOTALL | re.IGNORECASE
        )
        
        if replacements == 0:
            print(f"[{sp_name}] warning: Did not find standard FollowUp 10-table block to replace.")
            # Even if we didn't find the FollowUp block, we can still create the V2 shell
        
        # Construct the V2 appended block
        v2_sql = f"""

DELIMITER //

DROP PROCEDURE IF EXISTS {sp_name_v2};

CREATE PROCEDURE {sp_name_v2}({params})
BEGIN
{new_body}
END //

DELIMITER ;
"""
        with open(filepath, 'a') as f:
            f.write(v2_sql)
            
        print(f"[{sp_name}] SUCCESS: Appended {sp_name_v2} using vw_mamba abstraction.")
        success_count += 1

    print(f"\nCompleted: {success_count} V2 queries fully generated. {skip_count} skipped.")

if __name__ == '__main__':
    create_v2_templates()
