##AUXILIARY FUNCTIONS
import pandas as pd
import datetime
from collections import namedtuple
import numpy as np


def time_overlap(dt_targeted_time_range_start, dt_targeted_time_range_end):
    #checks if the passed range overlaps with the last 30 days.
    Range = namedtuple('Range', ['start', 'end'])
    dt_max_end = datetime.datetime.now()
    dt_max_start = dt_max_end - datetime.timedelta(30)
    r1 = Range(start=dt_max_start, end=dt_max_end)
    r2 = Range(start=dt_targeted_time_range_start, end=dt_targeted_time_range_end)
    latest_start = max(r1.start, r2.start)
    earliest_end = min(r1.end, r2.end)
    delta = (earliest_end - latest_start).days + 1
    overlap = max(0, delta)
    return overlap

def bbox_checker(targeted_bbox):
    #check if the targeted bbox is not too large?
    return True

def depth_range_checker(targeted_depth_range):
    #check if the targeted depth range is within the last not too large?
    return True

def time_range_checker(targeted_time_range):
    #check if the targeted time range is within the last 30 days (erddap limitation)
    result = True
    try:
        dt_targeted_time_range_start = datetime.datetime.strptime(targeted_time_range.split('/')[0],"%Y-%m-%dT%H:%M:%SZ")
        dt_targeted_time_range_end = datetime.datetime.strptime(targeted_time_range.split('/')[1],"%Y-%m-%dT%H:%M:%SZ")
        if dt_targeted_time_range_end > dt_targeted_time_range_start:
            overlap = time_overlap(dt_targeted_time_range_start, dt_targeted_time_range_end)
            if overlap == 0:
                result = False
                print("...error! Please check the time ranges requested. Remember that trough erddap services only the last 30 days of data are available.")
        else:
            result = False
            print("...error! Please check the time ranges requested. The targeted_time_range_end should be greater than the targeted_time_range_start")
    except Exception as e:
        result = False
        print("...error! Please check the time format. It shoudl be: %Y-%m-%dT%H:%M:%SZ. Example: 2021-10-05T00:00:00Z")
    return result

def source_checker(targeted_source):
    #check if the targeted_source is within the insitu tac official source list
    result = True
    official_list = ["BO","BA","DB","DC","FB","MO","TG","GL","ML","CT","PF","RE","RF","SF","TS","TX","XB","TE","SM","HF","SD","VA"]
    if targeted_source in official_list:
        pass
    else:
        result = False
        print("...error! The targeted source seems not to be on the official list.")
    return result


def parameter_checker(targeted_parameter):
    #check if the targeted_parameter is within the insitu tac official parameter list
    result = True
    official_parameter_list_url = 'https://archimer.ifremer.fr/doc/00422/53381/72334.xlsx'
    coords = ["TIME","LATITUDE","LONGITUDE","DEPH","PRES","BEAR","RNGE","FREQUENCY"]
    official_list = []
    try:    
        data = pd.read_excel(official_parameter_list_url,'Parameters', skiprows = 1, nrows = 115)
        official_list = [str(item).split(' ')[0] for item in data['variable name'].tolist() if str(item) != 'nan' and str(item) not in coords]
    except Exception as e:
        pass
    if len(official_list)>0:
        if targeted_parameter in official_list:
            pass
        else:
            result = False
            msg = "...error! The targeted parameter seems not to be on the official list."
    else:
        result = False
        msg = "...error! An error ocurred when reading the insitu tac official parameter list. Is this url correct?: "+official_parameter_list_url
    if not result:
       print(msg)
    return result

constrains_checkers = {
    'bbox':bbox_checker,
    'time_range': time_range_checker,
    'depth_range': depth_range_checker,
    'source': source_checker,
    'parameter': parameter_checker,
}

def constrains_checker(constrains, constrains_checkers):
    results = []
    for key,value in constrains.items():
        result = constrains_checkers[key](value)
        results.append(result)
    return not(False in results)

def request_url(constrains):
    targeted_time_range_start, targeted_time_range_end = constrains['time_range'].split('/')
    targeted_depth_range_start, targeted_depth_range_end = constrains['depth_range'].split('/')
    source = 'https://nrt.cmems-du.eu/erddap/tabledap/copernicus_GLO_insitu_nrt_%s.htmlTable'%(constrains['source'])
    attrs = '?time,latitude,longitude,DEPH,INSTITUTION,INSTITUTION_EDMO_CODE,PLATFORM_NAME,PLATFORM_CODE,%s,%s'%(constrains['parameter'], constrains['parameter']+'_QC')
    time_constrain = '&time>=%s&time<=%s'%(targeted_time_range_start,targeted_time_range_end)
    depth_constrain = '&DEPH>=%s&DEPH<=%s'%(targeted_depth_range_start,targeted_depth_range_end)
    area_constrain = '&latitude>=%s&latitude<=%s&longitude>=%s&longitude<=%s'%(str(constrains['bbox'][1]), str(constrains['bbox'][3]), str(constrains['bbox'][0]), str(constrains['bbox'][2]))
    url = source + attrs + time_constrain + depth_constrain +area_constrain
    return url

def built_request_url(constrains):
    #returns the request url based on the passed constrains
    if constrains_checker(constrains, constrains_checkers):
        response = request_url(constrains)
    else:
        response = "No request url could be built based on constrains. Please check again those."
    return response


def response_request_to_dataframe(request_url):
    try:
        df = pd.read_html(request_url, attrs = {'class': 'erd'})[0]
        attrs, attrs_units = df.columns.get_level_values(0).tolist(), df.columns.get_level_values(1).tolist()
        df.columns = df.columns.droplevel(1)
        df['PLATFORM_CODE'] = df['PLATFORM_CODE'].astype(str)
        df['INSTITUTION_EDMO_CODE'] = df['INSTITUTION_EDMO_CODE'].astype(str)
        qc_col = df.columns.values[-1]
        categorical_columns = df.select_dtypes(include=["object"]).columns.tolist()
        for col in categorical_columns:
            df[col] = np.where(df[col].str.contains('nan', case = False),np.nan,df[col])
            df[col] = np.where(df[col].str.contains('unknown', case = False),np.nan,df[col])
    except:
        print('Sorry!, nothing was found matching your search constrains. Set others and try again!')
        df = pd.DataFrame([], columns=[])
        attrs, attrs_units = [], []
    return df, attrs, attrs_units   
