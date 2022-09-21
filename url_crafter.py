# Helper function used to craft the Spotify Charts urls based on the countries provided 

def create_urls():

    # enter path to countries csv
    my_file = open("", "r")

    content = my_file.read()
    content = content.split("\n")

    countries = [i.split(",")[0].strip() for i in content]
    short = [i.split(",")[-1].strip() for i in content]
    urls= ["https://charts.spotify.com/charts/view/regional-" + i + "-weekly/latest" for i in short]

    dic = zip(countries,urls, short)
    my_file.close()
    return dic
