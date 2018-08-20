package someSite.controller;

import org.springframework.web.client.RestTemplate;
import someSite.dao.Dao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@CrossOrigin("http://localhost:8080")
@org.springframework.web.bind.annotation.RestController
public class RestController {

    @Autowired
    Dao dao;

    @RequestMapping(value = "/setColumns", method = RequestMethod.POST)
    void uploadMapping(@RequestBody String arrayMap[][]){
        dao.addColumns(arrayMap);
    }

    @RequestMapping(value = "/setValue", method = RequestMethod.POST)
    public void uploadData(@RequestBody  ArrayList<ArrayList<String>> arrayValue){

        dao.insertData(arrayValue);
    }

    @RequestMapping(value = "/getTransitTables", method = RequestMethod.GET)
    public List<Map<String, Object>> getTransitTables(){
        dao.createTransitTables();
        return dao.selectTransitTables();
    }

    @RequestMapping(value = "/uploadInToThird",method = RequestMethod.GET)
    public void upLoadIntoMainDB() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            restTemplate.postForObject("http://localhost:8082/setData",  dao.selectAll(), List.class);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @RequestMapping(value = "/setTransitTable/{tableName}", method = RequestMethod.GET)
    public void setTransitTable(@PathVariable("tableName") String tableName){
        dao.createTransitTables();
        dao.setTransitTableName(tableName);
        dao.setInToTransitTables(tableName);
        dao.deleteTable();
        dao.createTable();
    }

    @RequestMapping(value = "/doValidation", method = RequestMethod.POST)
    public ArrayList<Map<String, Object>>  doValidation (@RequestBody  ArrayList<Map> validation ){
        ArrayList<Map<String, Object>> reportsOfValidation = new ArrayList<>();

        for (Map map:validation) { //map.get("field")
            List<String> columns = new ArrayList<>();


            if (map.get("validationType").toString().toLowerCase().equals("system")){
                if (map.get("validation").toString().equals("is_Unique")){
                    if (map.get("field") == null) columns = dao.getColumns();
                    else columns.add(map.get("field").toString());
                    for (String field:columns) {
                        List<Map<String, Object>> result = dao.checkValidInFieldNameByUnique(field);
                        dao.upDateIsValidFalse(result);
                        for (Map resMap : result) {
                            Map<String, Object> map1 = dao.getReport("System validation",map.get("validation").toString(),
                                    resMap , map.get("table").toString(),columns.size(), result.size());
                            if (map1 == null) continue;
                            reportsOfValidation.add(map1);
                        }
                    }
                }
                else if (map.get("validation").toString().equals("is_Not_Null")){
                    if (map.get("field") == null) columns = dao.getColumns();
                    else columns.add(map.get("field").toString());
                    for (String field:columns) {
                        List<Map<String, Object>> result = dao.checkValidByNotNullField(field);
                        dao.upDateIsValidFalse(result);
                        for (Map resMap : result) {
                            reportsOfValidation.add(dao.getReport("System validation",map.get("validation").toString(),
                                    resMap, map.get("table").toString(), columns.size(),result.size()));
                        }
                    }
                }
                else {
                    if (map.get("field") == null) columns = dao.getColumns();
                    else columns.add(map.get("field").toString());
                    for (String field:columns) {
                        List<Map<String, Object>> result = dao.checkValidByIntField(field);
                        result =  dao.parseVarCharToInt(result);
                        dao.upDateIsValidFalse(result);
                        for (Map resMap : result) {
                            Map<String, Object> rep = dao.getReport("System validation",map.get("validation").toString(),
                                    resMap, map.get("table").toString(),columns.size(), result.size());
                            if (rep != null){
                                reportsOfValidation.add(rep);
                            }
                        }
                    }
                }
            }
            else{
                List<Map<String, Object>> result = dao.checkManualValidation(map.get("validation").toString());
                dao.upDateIsValidFalse(result);
                for (Map resMap : result) {
                    reportsOfValidation.add(dao.getReport("Manual validation", map.get("validation").toString(),
                            resMap, map.get("table").toString(),columns.size(),result.size()));
                }
            }
        }

        return reportsOfValidation;
    }

}

