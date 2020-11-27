package com.exportstaging.controller;

import com.exportstaging.api.domain.Attribute;
import com.exportstaging.api.implementation.ItemApiImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class GetAttributeController {
  
  @Autowired
  ItemApiImpl itemApiImpl;

  @RequestMapping(value = "/get/attribute", method = RequestMethod.GET)
  public List<Attribute> attributeById() throws Exception
  {
    return itemApiImpl.getAttribute(0, 0, "Pdmarticle");
  }
  
  @RequestMapping(value = "/get/attribute/{id}", method = RequestMethod.GET)
  public List<Attribute> attributeById(@PathVariable(value = "id") int id,
      @RequestParam(value = "lang", defaultValue = "de") String lang,
      @RequestParam(value = "depth", defaultValue = "0") int depth) throws Exception
  {
    return itemApiImpl.getAttribute(id, depth, "Pdmarticle");
  }
  
  @RequestMapping(value = "/get/attribute/byExternalKey/{type}/{attributeExternalkey}",
      method = RequestMethod.GET)
  public Attribute attributeByExternalKey(@PathVariable String type,
      @PathVariable String attributeExternalkey) throws Exception
  {
    return itemApiImpl.getAttributeByExternalKey(attributeExternalkey);
  }
  
  @RequestMapping(value = "/get/attribute/byClassId/{classId}", method = RequestMethod.GET)
  public List<Long> attributeByClassId(@PathVariable int classId) throws Exception
  {
    List<Long> list = itemApiImpl.getAttributeIDsByClassID(classId, "Pdmarticle");
    return list;
  }
  
}
