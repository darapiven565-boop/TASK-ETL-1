import pandas as pd
import numpy as np

pd.set_option("display.max_columns", 50) #прошу показувати всі колонки
pd.set_option("display.width", 100) #налаштовую скільки знаків він виводить в рядочку

url = "https://s3-eu-west-1.amazonaws.com/shanebucket/downloads/uk-500.csv"

df_origin = pd.read_csv(url)

COLUMNS_TO_DROP = []
print("---head---")
print(df_origin.head())
print("---info---")
print(df_origin.info())
print("---describe---")
print(df_origin.describe())
print("---describe for objects---")
print(df_origin.describe(include=[object]).T)
print("---null---")
# print(df.isna().sum())
print(df_origin.isna().sum().sort_values(ascending=False).head(20))
print("---dublicates---")
print(df_origin.duplicated().sum())
print("---columns---")
# print(list(df.columns))
for i, col in enumerate(df_origin.columns):
    print(f"{i:02d}. {col}")
# print(df.sample(10))

# очищення данних
df_origin["email"] = df_origin["email"].str.lower()
df_origin["web"] = df_origin["web"].str.lower()
# print(df.email)
df_origin["phone1"] = df_origin["phone1"].str.strip()
df_origin["phone2"] = df_origin["phone2"].str.strip()
# print(df.phone1)
print(df_origin.phone2)
print(df_origin.sample(10))

df = df_origin.copy()
if COLUMNS_TO_DROP:
    print("---delete columns in list---")
    df_origin.raw = df.drop(columns=[col for col in COLUMNS_TO_DROP if col in df.columns], errors="ignore")
else:
    print("Nothing to delete")
# print(df.columns)

def standartize_text(s):
    if pd.isna(s):
        return np.nan
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    s = " ".join(s.split())
    return s

possible_email_cols = [c for c in df.columns if "email" in c.lower()]
possible_web_cols = [c for c in df.columns if ("web" in c.lower()) or ("website" in c.lower()) or ("url" in c.lower())]
possible_phone_cols = [c for c in df.columns if ("phone" in c.lower()) or ("telephone" in c.lower()) or ("tel" in c.lower())]
possible_fax_cols = [c for c in df.columns if "fax" in c.lower()]

# print("\nPossible columns")
# print("Email col", possible_email_cols)
# print("Phone col", possible_phone_cols)
# print("Fax col", possible_fax_cols)
# print("Web col", possible_web_cols)

for col in df.select_dtypes(include=["object"]).columns:
    df[col] = df[col].apply(standartize_text)

for col in possible_email_cols:
    df[col] = df[col].str.lower()
for col in possible_web_cols:
    df[col] = df[col].str.lower()

#очищення номерів телефону

def clean_phone(x):
    if pd.isna(x):
        return np.nan
    s = str(x)
    s = s.strip()
    plus = "+" if s.startswith("+") else "" #запис умови в одну строчку 

    # digits = ""
    # for ch in x:
    #     if ch.isdigit():
    #         digit += ch
    
    digit = "".join(ch for ch in s if ch.isdigit())

    if digit == "":
        return np.nan
    
    return plus + digit

for col in possible_phone_cols + possible_fax_cols:
    df[col] = df[col].apply(clean_phone)

def title_if_str(s):
    if pd.isna(s):
        return np.nan
    return s.title()

possible_city_cols = [c for c in df.columns if c.lower() in ("city", "city_name", "town")]
possible_address_cols = [c for c in df.columns if c.lower() in ("address", "street")]
possible_name_cols = [c for c in df.columns if c.lower() in ("name", "first_name", "last_name", "second name", "company_name", )]
name_title = possible_city_cols + possible_address_cols + possible_name_cols
if name_title:
    for col in name_title:
        df[col] = df[col].apply(title_if_str)
    print("name of title")
else:
    print("no name")

#створення нових колонок

df["full_name"] = df.first_name + " " + df.last_name

df["domain"] = [c.split("@")[-1] for c in df.email]

# df["is_gmail"] = [True if "@gmail.com" in str(s).lower() else False for s in df["email"]]
df["is_gmail"] = [True if str(s).endswith("@gmail.com") else False for s in df["email"]]

# print(df.sample(10))

gmail_users = df.loc[df["is_gmail"] == True].copy()
# print("Gmail users", len(gmail_users))

# df["company_name"]
# def company_col():
df["company_name"] = df["company_name"].fillna("")

mask_LLC_Ltd = df.company_name.str.contains(r"\b(LLC|Ltd|llc|LTD|ltd)\b", regex=True, na=False)
company_LLC_Ltd = df.loc[mask_LLC_Ltd].copy()
# print(company_LLC_Ltd)
# print("companys LLC, Ltd", len(company_LLC_Ltd))

#позиційна вибірка

try:
    first_10_raw_2_col = df.iloc[:10, 2:6]
    # print(first_10_raw_2_col)
except Exception as e:
    print("не можна Перші 10 рядків + колонки", e)

every_10th = df.iloc[::10, :].copy()
# print(every_10th)

random_5 = df.sample(5, random_state=42)
# print(random_5)

#групування та статистика

top_domain = pd.DataFrame(df["email"].str.split("@").str[-1].value_counts().head(5))
# print(df["city"].value_counts().head(10))

# agg_by_city = df.groupby("city").agg(
#     people_count = ("city", "size"),
#     avg_people = ("first_name", "mean")
# )   #.sort_values(people_count).head(10)

# print(agg_by_city)


agg_by_city = df.groupby("city").agg(
    people_count =("first_name", "count"),
    uniq_domen = ("domain", "nunique")
).sort_values("people_count", ascending=False).head(10)
print(agg_by_city)

city = df.groupby("city")["first_name"].nunique()
count_by_city = df.groupby('city').size().reset_index(name='count').sort_values("count", ascending=False)
print(city)
print(count_by_city)

# Збереження данних

df.to_csv("data/uk500_clean.csv", encoding="utf-8")
gmail_users.to_csv("data/gmail_users.csv", encoding="utf-8")

file_name = 'stats.xlsx'

with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
    top_domain.to_excel(writer, sheet_name='domens', index=False)
    count_by_city.head(2).to_excel(writer, sheet_name='Cities', index=False)

