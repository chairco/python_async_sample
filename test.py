try:
    from .auto import FDC
except Exception as e:
    from auto import FDC


if __name__ == '__main__':
    fdc = FDC(glass_id='TL6AJ0HAV')
    print(fdc.get_array_pds())
