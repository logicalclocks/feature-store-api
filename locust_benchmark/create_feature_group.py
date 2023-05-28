from common.hopsworks_client import HopsworksClient

if __name__ == "__main__":

    hopsworks_client = HopsworksClient()
    fg = hopsworks_client.get_or_create_fg()
    hopsworks_client.insert_data(fg)
    hopsworks_client.close()
