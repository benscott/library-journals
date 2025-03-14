import re    


from journals.tasks.abbrv import AbbreviationsTask


def clean_text(text):
    """
    Replace double spaces with single spaces and double punctuation with single punctuation.
    
    Args:
        text (str): The input text to clean.
        
    Returns:
        str: The cleaned text.
    """
    # Replace double spaces with a single space
    text = re.sub(r'\s{2,}', ' ', text)
    
    # Replace double punctuation with a single punctuation
    text = re.sub(r'([!?.,;:])\1+', r'\1', text)

    # Add a space after a dot
    text = re.sub(r"(?<=\.)\s*(?=\S)", " ", text)
    
    return text    

  