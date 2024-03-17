from flask import Flask, render_template, request
import pandas as pd
import folium
from datetime import datetime, timedelta


app = Flask(__name__)

# Read the master data
df = pd.read_parquet('Master_Data.parquet')

df['Device ID'] = pd.to_numeric(df['Device ID'], errors='coerce').astype('Int64')

# Get unique device IDs
unique_device_ids = df['Device ID'].unique()

print(unique_device_ids)

# Convert device IDs to string for dropdown menu
device_id_options = unique_device_ids


def module_1(df):
    data = df['FuelLevel']
    hrlfc = df['HRLFC']
    
    window_size = 15
    
    # threshold = (20/tankCapacity) * 100
    
    num_windows = len(data) - window_size + 1

    
    ind = []
    std_i = []
    std_f = []
    lats =[]
    longs =[]

    for i in range(num_windows):
        window = data[i:i + window_size]
        window_2 = hrlfc[i:i + window_size]
        lat = df['Latitude']
        long= df['Longitude']
        
              
        f_3 = window_2[:6].iloc[2] if len(window_2) >= 6 else None
        l_3 = window_2[-6:].iloc[-2] if len(window_2) >= 6 else None
        hrlfc_diff = f_3 - l_3
        hrlfc_p = hrlfc_diff #/ tankCapacity
    
        f_6 = window[:6]
        l_6 = window[-6:]
        m_f_6 = sum(f_6) / len(f_6)
        std_f_6 = (sum((x - m_f_6) ** 2 for x in f_6) / len(f_6)) ** 0.5
    
        m_l_6 = sum(l_6) / len(l_6)
        std_l_6 = (sum((x - m_l_6) ** 2 for x in l_6) / len(l_6)) ** 0.5
    
        upper_bound = m_f_6 - std_f_6
        lower_bound = m_l_6 + std_l_6
    
        diff = upper_bound - lower_bound
        re = diff - hrlfc_p
        
        drain_cond = "D" if re > 7.7 else "N"
        
        if drain_cond == "D":
            ind.append([i, i+15])
            std_i.append(std_f_6)
            std_f.append(std_l_6)
            lats.append(lat)
            longs.append(long)
            
            
        
        f_index = []    
            
        for i in range(len(ind)):
            if i == 0 or ind[i][0] > ind[i-1][1]:
                f_index.append(ind[i])
                

    # print(filtered_indexes)    
    
    return f_index

def module_2(df):
    
    data = df['FuelLevel']
    hrlfc = df['HRLFC']
    # tnk = tankCapacity/100

    """Take cummulative difference of the hrlfc column"""
    
    df['cum_diff'] = (hrlfc.diff().cumsum())
    
    c_diff = df['cum_diff']
    
    """Now calculate virtual fuel level"""
    
    df['vfl'] = data + c_diff
    
    return df

        
god_time = '2000-01-01 00:00:00'
god_time = datetime.strptime(god_time, '%Y-%m-%d %H:%M:%S')  # Use strptime to convert string to datetime object
df["IST_DateTime"] = god_time + pd.to_timedelta(df["UTC"], unit='s') + timedelta(hours=5, minutes=30)

df['Device ID'] = pd.to_numeric(df['Device ID'], errors='coerce').astype('Int64')
df['FuelLevel'] = pd.to_numeric(df['FuelLevel'], errors='coerce').astype(float)
df['HRLFC'] = pd.to_numeric(df['HRLFC'], errors='coerce').astype(float)

df = df.reset_index()
df = df.drop(columns=['index'])

df = df.sort_values('Device ID')

df = df.dropna()


@app.route('/')
def index():
    return render_template('index.html', device_id_options=device_id_options)

@app.route('/generate_map', methods=['POST'])
def generate_map():

    mymap = folium.Map()

    selected_device_id = request.form['device_id']
    start_date = request.form['start_date']
    end_date = request.form['end_date']

    selected_device_id = int(selected_device_id)
    
    # Filter data for the selected device ID
    selected_data = df[df['Device ID'] == selected_device_id]


    start_date = start_date + ' ' + '00:00:00'  # start date by the customer
    end_date =  end_date + ' ' + '23:59:59'  # end date by the customer
        
    org_data = df[df['Device ID'] == selected_device_id]
    veh_data = org_data.sort_values('IST_DateTime')
        
    p_data = veh_data[(veh_data['IST_DateTime'] >= start_date) & (veh_data['IST_DateTime'] <= end_date)]
    if p_data.empty:
        raise ValueError('NO DATA OF THIS DATE RANGE')
    else:
        p_data.to_excel('duration_data_n.xlsx')
        
        p_data = pd.read_excel('duration_data_n.xlsx')
        
        fd = p_data
        
        fd = fd[fd['FuelLevel'] <= 100]  # consider only fuel Level under the desired limits
        
        #=========================MODULE 01 CODE=======================================
        
        m1 = module_1(fd)
        
        m2 = module_2(fd)
        
        new_fuel_level = pd.DataFrame(m2)
        
        new_fuel_level=new_fuel_level.drop(['FuelLevel'], axis=1)
        
        new_fuel_level.rename(columns={'vfl':'FuelLevel'}, inplace=True)
        
        m3 = module_1(new_fuel_level)
        
        mean_coords_list = []
        
        for index_range in m3:
            start_index, end_index = index_range
            latitudes = fd['Latitude'].iloc[start_index:end_index+1]
            longitudes = fd['Longitude'].iloc[start_index:end_index+1]
            mean_latitude = latitudes.mean()
            mean_longitude = longitudes.mean()
            mean_coords_list.append([mean_latitude, mean_longitude])
        
        
        
        if mean_coords_list:
            map_center = [sum(coord[0] for coord in mean_coords_list) / len(mean_coords_list),
                            sum(coord[1] for coord in mean_coords_list) / len(mean_coords_list)]
            mymap = folium.Map(location=map_center, zoom_start=10)
            for i, coords in enumerate(mean_coords_list):
                folium.Marker(location=coords, popup=f'Mean Point {i+1}').add_to(mymap)
                map_html = mymap._repr_html_()
                
                
    
            return render_template('index.html', device_id_options=device_id_options, map_html=map_html)
        
            
        else:
            return render_template('no_corr.html')

    # Convert the map to HTML representation

if __name__ == '__main__':
    app.run(debug=True)
