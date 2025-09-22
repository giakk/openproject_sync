def trasforma_stringa(stringa):
    """
    Trasforma una stringa dal formato 'N23/65' al formato 'n23-65'
    
    Args:
        stringa (str): Stringa nel formato 'N23/65'
    
    Returns:
        str: Stringa trasformata nel formato 'n23-65'
    """
    # Converte in minuscolo e sostituisce '/' con '-'
    return stringa.lower().replace('/', '-')


def trova_progetto_per_identifier(projects_data, identifier):
    """
    Trova un progetto dato l'identifier nel JSON dei progetti di OpenProject.
    
    Args:
        projects_data (dict): Il JSON restituito dalla chiamata API dei progetti
        identifier (str): L'identifier del progetto da cercare
    
    Returns:
        dict or None: Il progetto trovato o None se non esiste
    """
    # Verifica che i dati siano validi
    if not projects_data or '_embedded' not in projects_data:
        return None
    
    elements = projects_data['_embedded'].get('elements', [])
    
    # Ricerca diretta con next() per efficienza - si ferma al primo match
    return next((project for project in elements 
                if project.get('identifier') == identifier), None)


# Esempi di utilizzo
if __name__ == "__main__":
    # Test trasformazione stringa
    esempi = ['N23/65', 'N12/34', 'N99/01', 'N1/2']
    
    for esempio in esempi:
        risultato = trasforma_stringa(esempio)
        print(f"'{esempio}' -> '{risultato}'")
    
    # Test ricerca progetto (esempio con dati mock)
    mock_data = {
        '_embedded': {
            'elements': [
                {'id': 12, 'identifier': 'com020', 'name': 'COM020'},
                {'id': 14, 'identifier': 'n66-66', 'name': 'N66/66'},
                {'id': 13, 'identifier': 'n23-32', 'name': 'N23/32'}
            ]
        }
    }
    
    # Test ricerca
    progetto = trova_progetto_per_identifier(mock_data, 'n66-66')
    print(f"\nProgetto trovato: {progetto['name'] if progetto else 'Nessuno'}")
    
    progetto_inesistente = trova_progetto_per_identifier(mock_data, 'inesistente')
    print(f"Progetto inesistente: {progetto_inesistente}")