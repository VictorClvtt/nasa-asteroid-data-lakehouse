def drop_columns(df, cols_to_drop):
    for col in cols_to_drop:
        if col in df.columns:
            df = df.drop(col)
            print(f"✅ Column '{col}' dropped successfully!")
        else:
            print(f"❌ Column '{col}' not found!")

    return df