import json

# Load the scraped data
with open('scraped_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Count reactions per dataset
total_datasets = 0
total_reactions = 0
dataset_counts = []

for doi_prefix, doi_data in data.items():
    for doi_suffix, reactions in doi_data.items():
        reaction_count = len(reactions)
        dataset_id = f"{doi_prefix}{doi_suffix}"
        dataset_counts.append({
            'dataset': dataset_id,
            'reactions': reaction_count
        })
        total_datasets += 1
        total_reactions += reaction_count

# Sort by reaction count (descending)
dataset_counts.sort(key=lambda x: x['reactions'], reverse=True)

# Print summary
print(f"\n{'='*80}")
print(f"SUMMARY: {total_datasets} datasets, {total_reactions} total reactions")
print(f"{'='*80}\n")

# Print each dataset with reaction count
for item in dataset_counts:
    print(f"{item['dataset']}: {item['reactions']} reactions")

print(f"\n{'='*80}")
print(f"Total: {total_datasets} datasets, {total_reactions} reactions")
print(f"{'='*80}")

