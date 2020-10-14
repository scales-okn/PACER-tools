def nearest_ent_index(search_phrase, text, ents):
    '''
    Identifies nearest entity to a search phrase in a text block.
    Ents must be generated from text, search_phrase must be in text.
    input:
        * search_phrase -- str, regex to search for
        * text -- str, document text
        * ents -- list, list of spacy entities that should be considered in search
    output:
        * min_index -- int, index for the entity list of the closest spacy entity to search phrase
    '''
    import re 

    bspan, espan = re.search(search_phrase, text).span()
    #Subtract bspan, then we want the minimum distance that is positive
    start_chars = [ent.start_char - bspan for ent in ents]
    m = min(i for i in start_chars)
    min_index = start_chars.index(m)
    return min_index
