import arcpy, os, numpy as np, json
from arcpy import env
import time

start = time.time()
def time_elapsed():
    return round((time.time()-start),2)
print "start"
# Set environment settings
arcpy.env.overwriteOutput = True

dirpath = "C:/LandUseProject/codes/test/Union/pkg19"

fc_input_parcel = os.path.join(dirpath, "data/19_parcel.shp")
fc_input_lu = os.path.join(dirpath, "data/LU.shp")
fc_output_union = os.path.join(dirpath, "output/P_U_L/pkg19_parcel_Union_LU.shp")
fc_output_parcel = os.path.join(dirpath, "output/P_U_L/pkg19_parcel_PLU.shp")

print "Adding Fields P_AREA for %s", (fc_input_parcel)
arcpy.AddField_management(in_table=fc_input_parcel, field_name="P_AREA", field_type="DOUBLE")
print "OK elapsed %s seconds" %(time_elapsed())
print "Adding Fields LU_AREA for %s", (fc_input_lu)
arcpy.AddField_management(in_table=fc_input_lu, field_name="LU_AREA", field_type="DOUBLE")
print "OK elapsed %s seconds" %(time_elapsed())
print "CalculateField_management %s Field P_AREA" % (fc_input_parcel)
arcpy.CalculateField_management(fc_input_parcel, "P_AREA", "!shape.area@squaremeters!", "PYTHON")
print "OK elapsed %s seconds" %(time_elapsed())
print "CalculateField_management %s Field LU_AREA" % (fc_input_lu)
arcpy.CalculateField_management(fc_input_lu, "LU_AREA", "!shape.area@squaremeters!", "PYTHON")
print "OK elapsed %s seconds" %(time_elapsed())
print "creating Union of " + fc_input_parcel + " and " + fc_input_lu
in_features = [fc_input_parcel, fc_input_lu]
arcpy.Union_analysis(in_features,
                     fc_output_union,
                     join_attributes="ALL",
                     cluster_tolerance="#",
                     gaps="GAPS")

print "Saved Union to  %s " % (fc_output_union)
print "OK elapsed %s seconds" %(time_elapsed())

print "adding fields CAL_AREA, AREA_PER,OVERLAPS"
arcpy.AddField_management(in_table=fc_output_union, field_name="CAL_AREA", field_type="DOUBLE")
arcpy.AddField_management(in_table=fc_output_union, field_name="AREA_PER", field_type="DOUBLE")
print "OK elapsed %s seconds" %(time_elapsed())
print "calculating Fields for %s" % (fc_output_union) 
print "calculating Field CAL_AREA = !shape.area@squaremeters! "
arcpy.CalculateField_management(fc_output_union, "CAL_AREA", "!shape.area@squaremeters!", "PYTHON")
print "OK elapsed %s seconds" %(time_elapsed())

codeblock = """def cal(p,c):
                   if p>0:
                       return c/p*100
                   return -1"""
print "calculating Field AREA_PER using codeblock \n %s  " % (codeblock)
arcpy.CalculateField_management(in_table=fc_output_union,
                                field="AREA_PER",
                                expression="cal( !P_AREA! , !CAL_AREA!)",
                                expression_type="PYTHON_9.3",
                                code_block=codeblock)
print "OK elapsed %s seconds" %(time_elapsed())

fields = ['PARCELKEY', 'PARCELNO', 'AREA_PER', 'LEVEL1']
arr = arcpy.da.FeatureClassToNumPyArray(fc_output_union, ('PARCELKEY', 'PARCELNO', 'AREA_PER', 'LEVEL1'))

overlaps = {}

print "Calculating overlaps "
for a in arr:
    if a[0] in overlaps:
        if a[2] > 0:
            overlaps[a[0]].append([str(a[3]), round(a[2], 2)])
    else:
        if a[2] > 0:
            overlaps[a[0]] = [[str(a[3]), round(a[2], 2)]]

print "OK elapsed %s seconds" %(time_elapsed())
print "CopyFeatures %s to %s" % (fc_input_parcel, fc_output_parcel)
arcpy.CopyFeatures_management(fc_input_parcel, fc_output_parcel)
print "OK elapsed %s seconds" %(time_elapsed())
print "adding fields PLU_MAX, PLU_MAX_AR,PLU_ALL"
arcpy.AddField_management(in_table=fc_output_parcel, field_name="PLU_MAX", field_type="TEXT")
arcpy.AddField_management(in_table=fc_output_parcel, field_name="PLU_MAX_AR", field_type="DOUBLE")
arcpy.AddField_management(in_table=fc_output_parcel, field_name="PLU_ALL", field_type="TEXT")
print "OK elapsed %s seconds" %(time_elapsed())


def getstr_PLU_ALL(parcelkey, overlaps):
    if parcelkey in overlaps:
        # print overlaps[parcelkey]
        try:
            return json.dumps(overlaps[parcelkey])
        except TypeError as e:
            print "Error"
            print overlaps[parcelkey]
            pass
        return json.dumps([])


def getstr_PLU_MAX(parcelkey, overlaps):
    plu_distinct = {}
    if parcelkey in overlaps:
        plos = overlaps[parcelkey]
        for plo in plos:
            if plo[0] in plu_distinct:
                plu_distinct[plo[0]] = plu_distinct[plo[0]] + plo[1]
            else:
                plu_distinct[plo[0]] = plo[1]

    MaxDictVal = max(plu_distinct, key=plu_distinct.get)

    return MaxDictVal


def getstr_PLU_MAX_AREA(parcelkey, overlaps):
    plu_distinct = {}
    if parcelkey in overlaps:
        plos = overlaps[parcelkey]
        for plo in plos:
            if plo[0] in plu_distinct:
                plu_distinct[plo[0]] = plu_distinct[plo[0]] + plo[1]
            else:
                plu_distinct[plo[0]] = plo[1]

    MaxDictVal = max(plu_distinct, key=plu_distinct.get)

    return plu_distinct[MaxDictVal]


print "start writing values PLU_ALL,PLU_MAX,PLU_MAX_AR in " + fc_output_parcel
with arcpy.da.UpdateCursor(fc_output_parcel, ["PARCELKEY", "PLU_ALL", "PLU_MAX", "PLU_MAX_AR"]) as cursor:
    for row in cursor:
        if row[0] in overlaps:
            row[1] = getstr_PLU_ALL(row[0], overlaps)
            row[2] = getstr_PLU_MAX(row[0], overlaps)
            row[3] = getstr_PLU_MAX_AREA(row[0], overlaps)
            cursor.updateRow(row)
            plu_distinct = getstr_PLU_MAX(row[0], overlaps)
    cursor.reset();
    del cursor
print "OK elapsed %s seconds" %(time_elapsed())
print "COMPLETED!!"
