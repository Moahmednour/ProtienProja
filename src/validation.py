## validation.py
# Module for data validation and cleaning

def validate_sequence(sequence):
    """
    Validate a protein sequence to ensure it contains only valid amino acid characters.

    Parameters:
        sequence (str): Protein sequence.

    Returns:
        bool: True if valid, False otherwise.
    """
    valid_amino_acids = set("ACDEFGHIKLMNPQRSTVWY")
    return set(sequence).issubset(valid_amino_acids)

def validate_interpro(interpro_domains):
    """
    Validate InterPro domains to ensure they follow the format 'IPRXXXXX'.

    Parameters:
        interpro_domains (list): List of InterPro domain strings.

    Returns:
        list: List of valid InterPro domains.
    """
    return [domain for domain in interpro_domains if domain.startswith("IPR") and domain[3:].isdigit()]

def validate_row(row):
    """
    Validate a row to ensure it meets all required criteria.

    Parameters:
        row (dict): Dictionary representation of a data row.

    Returns:
        dict or None: Validated row or None if invalid.
    """
    # Validate sequence
    if not validate_sequence(row["Sequence"]):
        return None

    # Validate InterPro domains
    row["InterPro"] = validate_interpro(row["InterPro"])
    if not row["InterPro"]:
        return None

    return row

def normalize_interpro(row):
    """
    Normalize the InterPro column by ensuring unique entries.

    Parameters:
        row (dict): Dictionary representation of a data row.

    Returns:
        dict: Row with normalized InterPro domains.
    """
    row["InterPro"] = list(set(row["InterPro"]))
    return row
