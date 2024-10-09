import pandas as pd
import json
import time
from datetime import datetime
from dateutil.parser import isoparse
from supabase import create_client, Client

supabase_url = "https://tvjclbhclyozgziixpcp.supabase.co"
supabase_anon_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InR2amNsYmhjbHlvemd6aWl4cGNwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjUxMzI3NjIsImV4cCI6MjA0MDcwODc2Mn0.pXXAk6UADtwqU4JBho12F5OXStrvTG8pqk87xPiRvcg"

supabase: Client = create_client(supabase_url, supabase_anon_key)

launch = pd.DataFrame(supabase.table('list of initial schools for launch (recommend feature)').select('*').execute().data)
rankings = pd.DataFrame(supabase.table('major rankings by school (reccomend feature)').select('*').execute().data)
updated = pd.DataFrame(supabase.table('bio facts by school (reccomend feature)').select('*').execute().data)

last_created_at = isoparse("2024-09-13T20:09:37.294317+00:00")

def check_for_recent_rows(last_created_at):
    last_created_at_str = last_created_at.isoformat()
    
    response = supabase.table('player_profiles')\
        .select('*')\
        .gt('created_at', last_created_at_str)\
        .order('created_at', ascending=True)\
        .execute()
    
    return response.data


while True:
    new_rows = check_for_recent_rows(last_created_at)
    
    if new_rows:
        earliest_row = new_rows[0]
        
        created_at = isoparse(earliest_row['created_at'])
        print(f"Earliest new row created at: {created_at}")

        last_created_at = created_at


        for row in new_rows:
            player_data = row["stats"]
            student = pd.DataFrame([player_data])

            working_schools = updated.copy()
            initial_rec = []

            # Step 1: Drop schools the student can't get into academically
            boosted_sat = student["SAT"].values[0] + 200
            boosted_act = student["ACT"].values[0] + 5
            boosted_gpa = student["GPA"].values[0] + 0.3

            for index, row in working_schools.iterrows():
                school_sat = row["SAT Total"]
                school_act = row["ACT Composite"]
                school_gpa = row["Average GPA"]

                if boosted_gpa < school_gpa or boosted_act < school_act or boosted_sat < school_sat:
                    working_schools.drop(index, axis=0, inplace=True)

            # Step 2: Auto append a school that matches GPA
            student_major = student["Intended Major"].values[0] + " in"
            top_major_schools = rankings.loc[rankings["major"] == student_major]

            for index, row in working_schools.iterrows():
                if row["School Name"] in top_major_schools["school"].values:
                    initial_rec.append(row)  # Append the row directly
                    working_schools.drop(index, axis=0, inplace=True)

            # Step 3: Take into account school preferences
            student_body_pref = student["student_body_pop"].values[0]
            geography_pref = student["In-state?"].values[0]
            home_state = student["State"].values[0]
            financial_aid_qual = student["Aid Qual."].values[0]
            coa_pref = student["Cost Estimate"].values[0]
            greek_life_pref = student["Greek Life"].values[0]

            working_schools["Total Points"] = 0

            # Define classifiers for preferences
            def student_body_classifier(students):
                if students > 0 and students <= 5000:
                    return "Small"
                elif students > 5000 and students <= 12500:
                    return "Medium"
                elif students > 12500 and students <= 25000:
                    return "Large"
                elif students > 25000:
                    return "Very Large"
                return None

            def financial_aid_classifier(likelihood):
                if likelihood == "Yes":
                    return 1.00
                elif likelihood == "Probably Yes":
                    return 0.75
                elif likelihood == "Maybe":
                    return 0.5
                elif likelihood == "Probably No":
                    return 0.25  
                return 0

            def greek_life_classifier(likelihood):
                if likelihood == "Important":
                    return 60
                elif likelihood == "Somewhat Important":
                    return 55
                elif likelihood == "Indifferent":
                    return 45
                elif likelihood == "Somewhat Not Important":
                    return 35 
                return 0

            for index, row in working_schools.iterrows():
                points = 0

                if student_body_classifier(student_body_pref) == student_body_classifier(row["Undergraduates"]):
                    points += 1

                if geography_pref == "Yes" and row["State"] == home_state:
                    points += 1

                if financial_aid_classifier(financial_aid_qual) >= row["Average Percent of Need Met"]:
                    points += 1

                    if coa_pref + 7500 >= row["Cost of Attendance"] - row["Average Freshman Award"]:
                        points += 1

                school_greek_life = row["Fraternities"] + row["Sororities"]

                if greek_life_classifier(greek_life_pref) <= school_greek_life:
                    points += 1

                working_schools.at[index, "Total Points"] = points

            # Step 4: Filter based on current roster data
            # Implement your filtering logic here if needed

            # Step 5: Randomly recommend schools with the most points from different divisions
            ranked = working_schools.sort_values(by="Total Points", ascending=False)

            if not ranked.empty:
                top_school = ranked.iloc[0]   
                top_point = top_school["Total Points"]

                top_schools = ranked.loc[ranked["Total Points"] == top_point]
                top_schools_list = list(top_schools["School Name"])

                d1_dist = 0.65
                d2_dist = 0.15
                d3_dist = 0.20

                total_schools_needed = max(20 - len(initial_rec), 0)  # Ensure no negative value
                d1_count = int(total_schools_needed * d1_dist)
                d2_count = int(total_schools_needed * d2_dist)
                d3_count = total_schools_needed - (d1_count + d2_count)

                launch_subset = launch.loc[launch["School"].isin(top_schools_list)]

                if len(launch_subset[launch_subset['Division'] == 'NCAA D1']) < d1_count:
                    d1_count = len(launch_subset[launch_subset['Division'] == 'NCAA D1'])

                d1_schools = launch_subset[launch_subset['Division'] == 'NCAA D1'].sample(n=d1_count, replace=False)
                d2_schools = launch_subset[launch_subset['Division'] == 'NCAA D2'].sample(n=d2_count, replace=False)
                d3_schools = launch_subset[launch_subset['Division'] == 'NCAA D3'].sample(n=d3_count, replace=False)

                selected_schools = pd.concat([d1_schools, d2_schools, d3_schools])

                initial_rec = pd.concat([initial_rec, selected_schools], ignore_index=True)

            # Step 6: Prepare initial recommendations into JSON
            recs_json = json.dumps(initial_rec.to_dict(orient='records'))

            new_data = pd.DataFrame({
                "user_id": [row["user_id"]], 
                "schools": [recs_json]
            })

            # Step 7: Insert recommendations into the Supabase table
            response = supabase.table('initial_school_recs').insert(new_data).execute()

            # Optionally log the response or handle errors
            print(f"Inserted recommendation for user_id {user['user_id']}: {response}")
    else:
        print("No new rows found.")
    
    # Optionally add a delay before checking for new entries again
    time.sleep(5)  # Adjust as needed to control the loop frequency
