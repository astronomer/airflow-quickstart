import pandas as pd


def get_galaxy_data(num_galaxies: int = 20) -> pd.DataFrame:
    """
    Get data about up to 20 galaxies. This function mocks an API call.
    Args:
        num_galaxies: number of galaxies requested (default:20)
    """

    if num_galaxies > 20:
        print(
            "The maximum number galaxies for which data can be returned is 20. Returning data for 20 galaxies."
        )

    # hardcoded data for the example
    data = [
        {
            "name": "Canis Major Dwarf",
            "distance_from_milkyway": 42000,
            "distance_from_solarsystem": 25000,
            "type_of_galaxy": "Dwarf",
            "characteristics": "Closest galaxy",
        },
        {
            "name": "Sagittarius Dwarf Spheroidal",
            "distance_from_milkyway": 70000,
            "distance_from_solarsystem": 70000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "Being absorbed",
        },
        {
            "name": "Ursa Major II Dwarf",
            "distance_from_milkyway": 30000,
            "distance_from_solarsystem": 30000,
            "type_of_galaxy": "Dwarf",
            "characteristics": "Satellite of Milky Way",
        },
        {
            "name": "LGS 3",
            "distance_from_milkyway": 770000,
            "distance_from_solarsystem": 770000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "In Pisces",
        },
        {
            "name": "Small Magellanic Cloud",
            "distance_from_milkyway": 200000,
            "distance_from_solarsystem": 200000,
            "type_of_galaxy": "Dwarf",
            "characteristics": "Visible from Southern Hemisphere",
        },
        {
            "name": "Segue 1",
            "distance_from_milkyway": 23000,
            "distance_from_solarsystem": 23000,
            "type_of_galaxy": "Dwarf",
            "characteristics": "Dark matter dominated",
        },
        {
            "name": "Large Magellanic Cloud",
            "distance_from_milkyway": 163000,
            "distance_from_solarsystem": 163000,
            "type_of_galaxy": "Dwarf",
            "characteristics": "Visible from Southern Hemisphere",
        },
        {
            "name": "Bootes I",
            "distance_from_milkyway": 197000,
            "distance_from_solarsystem": 197000,
            "type_of_galaxy": "Dwarf",
            "characteristics": "Very faint",
        },
        {
            "name": "Draco Dwarf",
            "distance_from_milkyway": 260000,
            "distance_from_solarsystem": 260000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "Near the Draco Constellation",
        },
        {
            "name": "Sculptor Dwarf",
            "distance_from_milkyway": 290000,
            "distance_from_solarsystem": 290000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "Named after its constellation",
        },
        {
            "name": "Ursa Minor Dwarf",
            "distance_from_milkyway": 225000,
            "distance_from_solarsystem": 225000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "Near Ursa Minor",
        },
        {
            "name": "Carina Dwarf",
            "distance_from_milkyway": 330000,
            "distance_from_solarsystem": 330000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "In Carina constellation",
        },
        {
            "name": "Hercules Dwarf",
            "distance_from_milkyway": 550000,
            "distance_from_solarsystem": 550000,
            "type_of_galaxy": "Dwarf",
            "characteristics": "One of the most distant satellites",
        },
        {
            "name": "Fornax Dwarf",
            "distance_from_milkyway": 460000,
            "distance_from_solarsystem": 460000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "Named after its constellation",
        },
        {
            "name": "Leo I",
            "distance_from_milkyway": 820000,
            "distance_from_solarsystem": 820000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "Near Regulus",
        },
        {
            "name": "Phoenix Dwarf",
            "distance_from_milkyway": 1440000,
            "distance_from_solarsystem": 1440000,
            "type_of_galaxy": "Irregular",
            "characteristics": "Irregular shape",
        },
        {
            "name": "Leo II",
            "distance_from_milkyway": 690000,
            "distance_from_solarsystem": 690000,
            "type_of_galaxy": "Dwarf Spheroidal",
            "characteristics": "In Leo constellation",
        },
        {
            "name": "Barnard's Galaxy",
            "distance_from_milkyway": 1600000,
            "distance_from_solarsystem": 1600000,
            "type_of_galaxy": "Irregular",
            "characteristics": "Barred irregular",
        },
        {
            "name": "IC 10",
            "distance_from_milkyway": 2200000,
            "distance_from_solarsystem": 2200000,
            "type_of_galaxy": "Irregular",
            "characteristics": "Active star-forming galaxy",
        },
        {
            "name": "Andromeda Galaxy",
            "distance_from_milkyway": 2540000,
            "distance_from_solarsystem": 2540000,
            "type_of_galaxy": "Spiral",
            "characteristics": "predicted to collide with Milky Way in 4.5 billion years",
        },
    ]

    galaxies_df = pd.DataFrame(data)

    # return a random sample
    return galaxies_df.sample(num_galaxies)
