string = 'The Court made the followingORDER:Aggrieved by'

text_upper = string.upper()
print(text_upper)
procedural_phrases = [
        "DIRECTED TO", "LET THIS MATTER APPEAR", "WARNING LIST", "DAILY CAUSE LIST",
        "ADJOURNED", "LIST THE MATTER", "CALL FOR RECORDS", "PRODUCE THE RECORDS",
        "STAND OVER TO", "FIXED FOR", "POSTED FOR", "PUT UP ON", "MENTIONED BEFORE",
        "PLACE BEFORE", "TAKEN ON BOARD", "REFERRED TO", "TO BE LISTED",
        "TO BE PLACED", "RE-NOTIFY", "NOTIFIED FOR", "HEARING ON", "FOR ORDERS",
        "NEXT DATE OF HEARING", "ON THE NEXT DATE", "DIRECTED THAT",
        "REGISTRAR TO", "NOTICE TO", "ISSUE NOTICE", "RETURNABLE ON", "ORDER:",
        "LISTED FOR", "FURTHER ORDERS", "FOR MENTIONING", "THE COURT MADE THE FOLLOWING ORDER"
    ]
print('ORDER:' in text_upper)
if any(phrase in text_upper for phrase in procedural_phrases):
    print("order")