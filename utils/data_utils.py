from typing import List

import pandas as pd
from rapidfuzz import fuzz, process


def merge_dataframes(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Merge a list of DataFrames on their first columns."""
    if not dfs:
        return pd.DataFrame()

    merged_df = dfs[0]
    for df in dfs[1:]:
        left_col = merged_df.columns[0]
        right_col = df.columns[0]
        merged_df[left_col] = merged_df[left_col].fillna(0)
        merged_df = pd.merge(merged_df, df, how="left", left_on=left_col, right_on=right_col)
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
    merged_df = merged_df.T.drop_duplicates().T
    return merged_df


def format_column_names(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """
    Format DataFrame column names with consistent naming convention.
    Example: ('Playing Time', 'MP') -> 'Standard_Playing_Time_MP'
    """
    cols = df.columns.tolist()
    parsed_cols = []

    for col in cols:
        if "Unnamed" in col[0]:
            parsed_cols.append(col[1])
        elif col[0] == col[1]:
            parsed_cols.append(col[0])
        else:
            parsed_cols.append(f"{col[0]} {col[1]}")
    if key:
        df.columns = [f"{key.title()} {col.strip().replace(' ', '_')}" for col in parsed_cols]
    else:
        df.columns = [col.strip().replace(" ", "_") for col in parsed_cols]
    return df


def find_closest_matches(list1: list, list2: list, min_similarity: float = 0.6, case_sensitive: bool = True, allow_partial: bool = True) -> dict:
    """
    Find the best matches between two lists of strings using fuzzy matching.

    Args:
        list1: First list of strings to match
        list2: Second list of strings to match against
        min_similarity: Minimum similarity score (0.0 to 1.0) to consider a match
        case_sensitive: Whether to consider case when matching
        allow_partial: Whether to allow partial matches (substring matching)

    Returns:
        Dictionary mapping items from list1 to their best matches in list2
    """

    if not list1 or not list2:
        return {}

    # Clean and prepare lists
    clean_list1 = [str(item).strip() if item is not None else "" for item in list1]
    clean_list2 = [str(item).strip() if item is not None else "" for item in list2]

    if not case_sensitive:
        clean_list1 = [item.lower() for item in clean_list1]
        clean_list2 = [item.lower() for item in clean_list2]

    matches = {}
    used_indices = set()

    # First pass: exact matches
    for i, item1 in enumerate(clean_list1):
        if not item1:  # Skip empty strings
            continue
        for j, item2 in enumerate(clean_list2):
            if j in used_indices or not item2:
                continue
            if item1 == item2:
                matches[list1[i]] = list2[j]
                used_indices.add(j)
                break

    # Second pass: fuzzy matching for unmatched items
    unmatched_indices1 = [i for i, item in enumerate(list1) if item not in matches]
    available_list2 = [(j, item) for j, item in enumerate(list2) if j not in used_indices]

    for i in unmatched_indices1:
        item1 = clean_list1[i]
        if not item1:
            continue

        best_match = None
        best_score = 0
        best_index = -1

        for j, item2 in available_list2:
            clean_item2 = clean_list2[j]
            if not clean_item2:
                continue

            # Use multiple similarity measures
            if allow_partial:
                # Partial ratio for substring matching
                score = max(fuzz.ratio(item1, clean_item2), fuzz.partial_ratio(item1, clean_item2), fuzz.token_sort_ratio(item1, clean_item2)) / 100.0
            else:
                score = fuzz.ratio(item1, clean_item2) / 100.0

            if score >= min_similarity and score > best_score:
                best_match = list2[j]
                best_score = score
                best_index = j

        if best_match is not None:
            matches[list1[i]] = best_match
            available_list2 = [(j, item) for j, item in available_list2 if j != best_index]

    return matches


def find_most_similar_string(string: str, list_of_strings: list, min_similarity: float = 0.6, case_sensitive: bool = True):
    """
    Find the most similar string(s) from a list of candidates.

    Args:
        string: The string to find matches for
        list_of_strings: List of candidate strings
        min_similarity: Minimum similarity score (0.0 to 1.0) to consider a match
        case_sensitive: Whether to consider case when matching

    Returns:
        Tuple of (match, score)

    Raises:
        ValueError: If string or list_of_strings is empty, or if no match found above min_similarity
    """
    if not string or not list_of_strings:
        raise ValueError("Input string and list_of_strings cannot be empty")

    # Clean input
    clean_string = str(string).strip()
    clean_list = [str(item).strip() if item is not None else "" for item in list_of_strings]

    if not case_sensitive:
        clean_string = clean_string.lower()
        clean_list = [item.lower() for item in clean_list]

    # Get best matches with scores
    results = process.extract(clean_string, list_of_strings, scorer=fuzz.WRatio)

    # Filter by minimum similarity
    filtered_results = [(match, score / 100.0) for match, score, _ in results if score / 100.0 >= min_similarity]

    if not filtered_results:
        raise ValueError(f"No matches found for {string} in {list_of_strings}")

    match, score = filtered_results[0]
    return match
