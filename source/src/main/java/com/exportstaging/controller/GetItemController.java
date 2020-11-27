package com.exportstaging.controller;

import com.exportstaging.api.implementation.ItemApiImpl;
import com.exportstaging.api.wraper.RecordWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
public class GetItemController {

    @Autowired
    ItemApiImpl itemApiImpl;

    @RequestMapping(value = "/get/item/{type}/{itemId}", method = RequestMethod.GET)
    public RecordWrapper itemById(@PathVariable(value = "type") String type,
                                  @PathVariable(value = "itemId") int itemId) throws Exception {
        return itemApiImpl.getItemById(type, itemId);
    }

    @RequestMapping(value = "/get/item/byExternalKey/{type}/{itemExternalKey}",
            method = RequestMethod.GET)
    public RecordWrapper byExternalKey(@PathVariable String type, @PathVariable String itemExternalKey)
            throws Exception {
        return itemApiImpl.getItemByExternalKey(type, itemExternalKey);
    }

    @RequestMapping(value = "/get/item/idsByFilters/{type}", method = RequestMethod.GET)
    public Set<Integer> idsByFilters(@PathVariable(value = "type") String type,
                                    @RequestParam(value = "itemId", defaultValue = "") String itemId,
                                    @RequestParam(value = "workflowId", defaultValue = "") String workflowId,
                                    @RequestParam(value = "stateId", defaultValue = "") String stateId,
                                    @RequestParam(value = "configurationId", defaultValue = "") String pdmArticleConfigurationId,
                                    @RequestParam(value = "parentId", defaultValue = "") String parentId,
                                    @RequestParam(value = "lastChange", defaultValue = "") String lastChange,
                                    @RequestParam(value = "shortName", defaultValue = "") String languageShortName,
                                    @RequestParam(value = "languageId", defaultValue = "") String languageId,
                                    @RequestParam(value = "label", defaultValue = "") String label,
                                    @RequestParam(value = "isFolder", defaultValue = "") String isFolder,
                                    @RequestParam(value = "externalKey", defaultValue = "") String externalKey,
                                    @RequestParam(value = "creationDate", defaultValue = "") String creationDate,
                                    @RequestParam(value = "copyOf", defaultValue = "") String copyOf,
                                    @RequestParam(value = "checkoutUser", defaultValue = "") String checkoutUser,
                                    @RequestParam(value = "authorId", defaultValue = "") String authorId,
                                    @RequestParam(value = "author", defaultValue = "") String author,
                                    @RequestParam(value = "language", defaultValue = "default") String tempLanguage)
            throws Exception {
        Boolean language = false;
        if (tempLanguage != "default") {
            language = Boolean.getBoolean(tempLanguage);
        }

        /*SearchFilterCriteria searchFilterCriteria = new SearchFilterCriteria(itemId, workflowId,
                stateId, pdmArticleConfigurationId, parentId, lastChange, languageShortName, languageId,
                label, isFolder, externalKey, creationDate, copyOf, checkoutUser, authorId, author,
                language);*/
//        Set<Integer> set = itemApiImpl.getItemIdsByFilters(type, searchFilterCriteria);
        return null;
    }

}
