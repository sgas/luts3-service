"""
Module for converting a number to base62.
"""

BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"



def base10to62(number):

    BASE = 62

    if number == 0:
        return BASE62[number]

    new_number = ''
    current = number

    while current !=0 :
        remainder = current % BASE
        new_number += BASE62[remainder]
        current = current / BASE

    return ''.join( reversed(new_number) )

