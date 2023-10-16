<?php

namespace Customer\Amwins\Services\Document\Processes\ReliancePopulated;

class ImportWorker extends \App\Document\Services\Spooling\ImportWorker
{
    private
        $application_lookup,
        $productcategory_lookup,
        $fields;

    protected function setup()
    {
        parent::setup();
        // Ensures that any setup operations in the parent class are executed before.

        // Preload lookups

        // application_lookup is assigned the result of calling the all method it retrives all records from the specifield table
        // and then the method toArray is used to convert these records into an array format


        $this->application_lookup = \Customer\Amwins\Models\ReliancePopulatedApplicationLookup::all()->toArray();
        // same processs to save product category lookup
        $this->productcategory_lookup = \Customer\Amwins\Models\ReliancePopulatedProductCategoryLookup::all()->toArray();

        // an array of field names is created and assinged, these field names are related to the DATA OR FIELDS

        $this->fields = array(
            'rs_policyholder_name',
            'rs_situs_state',
            'rs_fed._employer_id#',
            'rs_employee_full_name',
            'rs_employee_gender',
            'rs_employee_ssn',
            'rs_employee_dob',
            'rs_employee_doh',
            'rs_annual_salary',
            'rs_occupation',
            'rs_date_of_last_salary_change',
            'rs_scheduled_work_hours/week',
            'rs_premium_paid_to_date',
            'rs_date_last_worked',
            'rs_date_of_qualifying_life_event_(qle)',
            'rs_qualifying_life_event_(qle)',
            'rs_employee_street_address_(1)',
            'rs_employee_street_address_(2)',
            'rs_employee_city',
            'rs_employee_state',
            'rs_employee_zip',
            'rs_dependent_spouse_full_name',
            'rs_dependent_child_full_name_(1)',
            'rs_dependent_child_full_name_(2)',
            'rs_dependent_child_full_name_(3)',
            'rs_policy_#_var',
            'rs_var_coverage_employee',
            'rs_var_coverage_spouse',
            'rs_var_coverage-_child(ren)',
            'rs_policy_#_gl',
            'rs_policy_effective_date',
            'rs_gl_class',
            'rs_life_effective_date_of_employee',
            'rs_life_effective_date_of_spouse',
            'rs_life_effective_date_of_child(ren)',
            'rs_basic_life_coverage_employee',
            'rs_basic_ad_&_d_coverage_employee',
            'rs_supplemental_ad_&_d_employee',
            'rs_dep._life_coverage_spouse',
            'rs_dep._life_coverage_child(ren)',
            'rs_supp._life_coverage_employee',
            'rs_supp._dep._coverage_spouse',
            'rs_supp._dep._coverage_child(ren)',
            'rs_policy_#_ltd',
            'rs_ltd_class',
            'rs_ltd_effective_date',
            'rs_policy_#_vpl',
            'rs_vpl_class',
            'rs_vpl_effective_date',
            'rs_policy_#_vg',
            'rs_vg_effective_date',
            'rs_vg_class',
            'rs_vg_coverage_employee',
            'rs_vg_effective_date_employee',
            'rs_vg_coverage_spouse',
            'rs_vg_effective_date_spouse',
            'rs_vg_coverage_child(ren)',
            'rs_vg_effective_date_child(ren)',
            'rs_policy_#_vci',
            'rs_vci_class',
            'rs_vci_coverage_employee',
            'rs_vci_effective_date_employee',
            'rs_vci_coverage_spouse',
            'rs_vci_effective_date_spouse',
            'rs_vci_coverage_child(ren)',
            'rs_vci_effective_date_child(ren)',
            'rs_policy_#_vai',
            'rs_vai_class',
            'rs_vai_plan',
            'rs_vai_coverage_type',
            'rs_vai_effective_date_employee',
            'rs_vai_effective_date_spouse',
            'rs_vai_effective_date_child(ren)',
            'rs_policy_#_vhi',
            'rs_vhi_plan_option',
            'rs_vhi_coverage_tier',
            'rs_vhi_effective_date_employee',
            'rs_vhi_effective_date_spouse',
            'rs_vhi_effective_date_child(ren)'
        );
    }


    protected function processRecord($record, $seed = null)
    {

        // the function  processRecord is defined as protected method wihtin a class. it takes two parameteres
        //records and seed optional and initialized as null

        $error = '';
        $success = false;

//these variables error and success will be used to track error messages and the success status of the processing


        $data = $record['data'];
        $this->filename = $record['filename'];
        // data  and fileName from the $record and the name associated with the record are extracted and stored


        $class_id_override = null;
        $class_id_override_name = null;

        $profileData = [];
        //declare profileDate array to store data related to the record being processed


        for ($i = 0; $i < count($this->fields); $i++) {
            $profileData[$this->fields[$i]] = trim($data[$i]);
        }

        //this loop iterates through the elements in the field array and extracts correxponding data from the data aray.
        //it trims the data, removes leading and trailing whitespaces, and stores in profile data


        // Establish campaign id
        $error = '';
        $status = 'success';

        $pageCount = 0;
        $imageCount = 0;
        //variables used to track of the counts realted to pages and images


        $touch_uni_number = $profileData['rs_employee_ssn'] .
            $profileData['rs_policy_#_var'] .
            $profileData['rs_policy_#_gl'] .
            $profileData['rs_policy_#_vg'] .
            $profileData['rs_date_of_qualifying_life_event_(qle)'] .
            $profileData['rs_qualifying_life_event_(qle)'];
        //touch_uni_number is constructed by concatenating values from various field in the profileData array.

        $touch_uni_number = str_replace(' ', '', $touch_uni_number);
        //any space are removed and the string is limited to 20 characters the result is stoerd in touch_uni_number

        $touch_uni_number = substr($touch_uni_number, 0, 20);
        $uni_number = $touch_uni_number;
        $profileData['n_number'] = $touch_uni_number;

        //any space


        $touch_uni_number_sub = $profileData['rs_employee_full_name'] . $profileData['rs_employee_street_address_(1)'] . $profileData['rs_employee_street_address_(2)'];

        //This will hold the main (first) touch_id

        //another unique identifier is constructed by concatenating the employess name and address

        $touch_id = null;
        $touch_id_secondary = null;

//touch_id and touch_id_secondary are initialized to nill, they eill be used to hold identifiers
        // Lookup Logic
        $productCategoryMatch = false;

        $profileData['rs_productcategory_lookup'] = [];
        $profileData['rs_electionperiod_lookup'] = [];

        // the goal is to match each policy with an application, resulting in an associative array, $profileData['rs_applicationform_lookup'],
        // each parent node is named by the policy number and each policy node has 5 fields policy, application, file_revision_date, productcategory, electionperiod
        // Build array of policy #'s to look up

        // two arrays are created, these array will be used to store data related to them


        if (trim($profileData['rs_policy_#_ltd'])) {
            $policy_data[$profileData['rs_policy_#_ltd']] = trim($profileData['rs_policy_#_ltd']);
        }
        if (trim($profileData['rs_policy_#_var'])) {
            $policy_data[$profileData['rs_policy_#_var']] = trim($profileData['rs_policy_#_var']);
        }
        if (trim($profileData['rs_policy_#_vpl'])) {
            $policy_data[$profileData['rs_policy_#_vpl']] = trim($profileData['rs_policy_#_vpl']);
        }
        if (trim($profileData['rs_policy_#_vg'])) {
            $policy_data[$profileData['rs_policy_#_vg']] = trim($profileData['rs_policy_#_vg']);
        }
        if (trim($profileData['rs_policy_#_vci'])) {
            $policy_data[$profileData['rs_policy_#_vci']] = trim($profileData['rs_policy_#_vci']);
        }
        if (trim($profileData['rs_policy_#_vai'])) {
            $policy_data[$profileData['rs_policy_#_vai']] = trim($profileData['rs_policy_#_vai']);
        }
        if (trim($profileData['rs_policy_#_gl'])) {
            $policy_data[$profileData['rs_policy_#_gl']] = trim($profileData['rs_policy_#_gl']);
        }
        if (trim($profileData['rs_policy_#_vhi'])) {
            $policy_data[$profileData['rs_policy_#_vhi']] = trim($profileData['rs_policy_#_vhi']);
        }

        /*
         * These if statements are used to build an array named $profileData,
                each if statement checks if a specific policy related field in profileData is not empty after trimming,
                 if the condition is met, it adds an entry to the policy_data array with the policy number as the key and the
                trimmed policy number as the value
                if statements are used to construct an associative array named  $policy_data which is a collectioon of policy
                numbers extracted from different fields in hte $profileData array. It includes various  policy numbers associated
                with the record being proccessed
        */


        $missingRequired = '';

        // Iterate through the policy's, could be 1 could be up to 8
        $i = 0;
        $onePolicyMatched = false;
        $onePolicyDidntMatch = false;
        $qleNotMatchedInfoText = '';

        //these variables will be used to keep track  of the information during the iteration  and matching process


        foreach ($policy_data as $policy) {

            /*
             the foreach loop begins, iterating through the values in the policy_data array.
             This loop is used to procees
             */


            $found = false;
            $qleFound = false;
            $currentPolicyMatch = false;
            $trackPolicyDups = [];

            // these variables  are used to track different conditions during policy matching


            foreach ($this->productcategory_lookup as $pcl) {
                /*
                inside this nested loop, various conditions are checked to determine if a match exist
                between policy data and product category data, if a match is found, relevant data is stored in the
                $profileData array
                */

                $rs_productcategory_lookup = trim($pcl['productcategory']);
                /*the conditions are compared
                the polycyholder names are compared, disregarding case and whitespace.
                the policy numbers are compared againg ignoring case and white space
                the situs states are compared

                if these conditios are met, it indicates a match between a policy and product category lookup.
                variables are uptadted accordingly
                */
                if (
                    trim(strtoupper($profileData['rs_policyholder_name'])) == trim(strtoupper($pcl['policyholder'])) &&
                    trim($policy) == trim($pcl['policy']) && trim($profileData['rs_situs_state']) == trim($pcl['state'])
                ) {
                    $currentPolicyMatch = true;
                    $onePolicyMatched = true;
                    if (trim(strtoupper($pcl['qle'])) != "ALL") {
                        /*
                         * if the QLE field in the product category is not set to "ALL" it indicates that specific QLEs are supported
                         *
                         */
                        $qle = explode(',', $pcl['qle']);

                        /*
                         * check if QLE from the policy data matches any of the QLEs supported by the product category
                         * if a match is found it proceeds to further lookup and processing
                         */
                        foreach ($qle as $q) {
                            // if we match any of the $q values we use those for lookup2
                            if (trim(strtoupper($q)) == trim(strtoupper($profileData['rs_qualifying_life_event_(qle)']))) {
                                $qleFound = true;

                                $stateFound = false;
                                foreach ($this->application_lookup as $al) {
                                    /*
                                    if a match is found, relevant data is stored in the profileData array under the policy key
                                    including policy information, aplication data, file revision date, product, category and election period


                                    */

                                    if (
                                        trim(strtoupper($rs_productcategory_lookup)) == trim(strtoupper($al['productcategory'])) &&
                                        trim($profileData['rs_situs_state']) == trim(strtoupper($al['situsstate']))
                                    ) {
                                        if (!$trackPolicyDups[$al['productcategory']]) {
                                            $trackPolicyDups[$al['productcategory']] = true;
                                            $profileData['rs_applicationform_lookup'][$policy][$i]['policy'] = $policy;
                                            $profileData['rs_applicationform_lookup'][$policy][$i]['application'] = $al['application'];
                                            $profileData['rs_applicationform_lookup'][$policy][$i]['file_revision_date'] = $al['file_revision_date'];
                                            $profileData['rs_applicationform_lookup'][$policy][$i]['productcategory'] = $rs_productcategory_lookup;
                                            $profileData['rs_applicationform_lookup'][$policy][$i]['electionperiod'] = trim($pcl['electionperiod']);
                                            $found = true;
                                            $stateFound = true;
                                            $i++;
                                        }
                                    }
                                }
                                if (!$stateFound) {
                                    foreach ($this->application_lookup as $al) {
                                        if (
                                            trim(strtoupper($rs_productcategory_lookup)) == trim(strtoupper($al['productcategory'])) &&
                                            trim($al['situsstate']) == 'Generic'
                                        ) {
                                            /*
                                                * to avoid duplicats it checks if the product category has alredy been
                                                * processed for the given policy,
                                                * IF NOT , it adds the PRODUCT CATEGORY DATA to the $profule data array
                                                *
                                                */
                                            if (!$trackPolicyDups[$al['productcategory']]) {
                                                $trackPolicyDups[$al['productcategory']] = true;
                                                $profileData['rs_applicationform_lookup'][$policy][$i]['policy'] = $policy;
                                                $profileData['rs_applicationform_lookup'][$policy][$i]['application'] = $al['application'];
                                                $profileData['rs_applicationform_lookup'][$policy][$i]['file_revision_date'] = $al['file_revision_date'];
                                                $profileData['rs_applicationform_lookup'][$policy][$i]['productcategory'] = $rs_productcategory_lookup;
                                                $profileData['rs_applicationform_lookup'][$policy][$i]['electionperiod'] = trim($pcl['electionperiod']);
                                                $found = true;
                                                $i++;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // if the above did not work try the qle All product category scenerio as a fallback for lookup1
                    } elseif (strtoupper(trim($pcl['qle'])) == "ALL") {

                        $qleFound = true;

                        $stateFound = false;
                        foreach ($this->application_lookup as $al) {
                            if (
                                trim(strtoupper($rs_productcategory_lookup)) == trim(strtoupper($al['productcategory'])) &&
                                trim($profileData['rs_situs_state']) == trim(strtoupper($al['situsstate']))
                            ) {
                                if (!$trackPolicyDups[$al['productcategory']]) {
                                    // error_log(" - Test 5 MATCH!!!");
                                    $trackPolicyDups[$al['productcategory']] = true;
                                    $profileData['rs_applicationform_lookup'][$policy][$i]['policy'] = $policy;
                                    $profileData['rs_applicationform_lookup'][$policy][$i]['application'] = $al['application'];
                                    $profileData['rs_applicationform_lookup'][$policy][$i]['file_revision_date'] = $al['file_revision_date'];
                                    $profileData['rs_applicationform_lookup'][$policy][$i]['productcategory'] = $pcl['productcategory'];
                                    $profileData['rs_applicationform_lookup'][$policy][$i]['electionperiod'] = trim($pcl['electionperiod']);
                                    $found = true;
                                    $stateFound = true;
                                    $i++;
                                }
                            }
                        }

                        if (!$stateFound) {
                            foreach ($this->application_lookup as $al) {
                                // if the above situs state does not match try the Generic State scenario below
                                if (
                                    trim(strtoupper($rs_productcategory_lookup)) == trim(strtoupper($al['productcategory'])) &&
                                    trim($al['situsstate']) == 'Generic'
                                ) {
                                    if (!$trackPolicyDups[$al['productcategory']]) {
                                        $trackPolicyDups[$al['productcategory']] = true;
                                        $profileData['rs_applicationform_lookup'][$policy][$i]['policy'] = $policy;
                                        $profileData['rs_applicationform_lookup'][$policy][$i]['application'] = $al['application'];
                                        $profileData['rs_applicationform_lookup'][$policy][$i]['file_revision_date'] = $al['file_revision_date'];
                                        $profileData['rs_applicationform_lookup'][$policy][$i]['productcategory'] = $pcl['productcategory'];
                                        $profileData['rs_applicationform_lookup'][$policy][$i]['electionperiod'] = trim($pcl['electionperiod']);
                                        $found = true;
                                        $i++;
                                    }
                                }
                            }
                        }
                    }
                    if (!$qleFound) {
                        $qleNotMatchedInfoText .= 'Policy ' . trim($policy) . ' was not produced due to QLE of "' .
                            $profileData['rs_qualifying_life_event_(qle)'] . '" not supported on product category lookup' . PHP_EOL;
                    }
                }
                if (!$currentPolicyMatch) {
                    $onePolicyDidntMatch = true;
                }
            }


            if (!$found) {
                $missingRequired .= "Failed Lookup - POLICY:$policy NAME:" . $profileData['rs_policyholder_name'] .
                    " STATE:" . $profileData['rs_situs_state'] . " QLE:" . $profileData['rs_qualifying_life_event_(qle)'] . " CATEGORY:$rs_productcategory_lookup" . PHP_EOL;
            }

            /*
             * check if a specific policy did not match with any product category. if it did not, it
             * appends an error message
             * The message includes details such as the policy number, policyhodlername, situs state, QLE
             */


        } // end policy loop

        //check if the all policies matched with product categories and if so, updates the $error variable
        if (!$onePolicyDidntMatch) {
            $error = $missingRequired;
            $missingRequired = '';
        }
        /*
         * if all policies successfully matched with product categorie, it updates $error variable
         * with the constens of the $missingRequired variable
         * it also resets the $missing variable
         *
         */


        if (!$onePolicyMatched) {
            $missingRequired = 'No policy numbers found in the lookup';
        } /*
                  * check if at least one policy matchet with a product category
                  * if no policy matched with anyu product category it sets the $missinRequired variable to
                  * the message 'No policy numbers found in the lookup
                  *
                  */


        /*
         * it serializes specific field in the $profileData array.
         * Serialization is the process of  converting data into a format that can be easily sorted and later
            */


        $profileData['rs_productcategory_lookup'] = serialize($profileData['rs_productcategory_lookup']);
        $profileData['rs_electionperiod_lookup'] = serialize($profileData['rs_electionperiod_lookup']);
        $profileData['rs_applicationform_lookup'] = serialize($profileData['rs_applicationform_lookup']);

        // Required Logic
        /*
         * the code check if a match was found for the policyu within the product categories
         * if a match WAS NOT FOUND  for a specific policy in the product categories, it logs an error message.
         * This message includes details like the policyu numberm policyHolder name, situs state
         * , QLE, and the product category. This error message is appended to the $missingrequired variable
         *
         */


        //The code performs a series of checks to ensure that certain fiels are not missing

        if ($profileData['rs_policyholder_name'] === '') {
            $missingRequired .= "Missing Field: rs_policyholder_name" . PHP_EOL;
        } else if ($profileData['rs_situs_state'] === '') {
            $missingRequired .= "Missing Field: rs_situs_state" . PHP_EOL;
        } else if ($profileData['rs_fed._employer_id#'] === '') {
            $missingRequired .= "Missing Field: rs_fed._employer_id#" . PHP_EOL;
        } else if ($profileData['rs_employee_full_name'] === '') {
            $missingRequired .= "Missing Field: rs_employee_full_name" . PHP_EOL;
        } else if ($profileData['rs_employee_ssn'] === '') {
            $missingRequired .= "Missing Field: rs_employee_ssn" . PHP_EOL;
        } else if ($profileData['rs_employee_dob'] === '') {
            $missingRequired .= "Missing Field: rs_employee_dob" . PHP_EOL;
        } else if ($profileData['rs_employee_doh'] === '') {
            $missingRequired .= "Missing Field: rs_employee_doh" . PHP_EOL;
        } else if ($profileData['rs_date_of_qualifying_life_event_(qle)'] === '') {
            $missingRequired .= "Missing Field: rs_date_of_qualifying_life_event_(qle)" . PHP_EOL;
        } else if ($profileData['rs_qualifying_life_event_(qle)'] === '') {
            $missingRequired .= "Missing Field: rs_qualifying_life_event_(qle)" . PHP_EOL;
        } else if ($profileData['rs_employee_street_address_(1)'] === '') {
            $missingRequired .= "Missing Field: rs_employee_street_address_(1)" . PHP_EOL;
        } else if ($profileData['rs_employee_city'] === '') {
            $missingRequired .= "Missing Field: rs_employee_city" . PHP_EOL;
        } else if ($profileData['rs_employee_state'] === '') {
            $missingRequired .= "Missing Field: rs_employee_state" . PHP_EOL;
        } else if ($profileData['rs_employee_zip'] === '') {
            $missingRequired .= "Missing Field: rs_employee_zip" . PHP_EOL;


            /*
             * checks id the policyholder_name field in the data is empty, and if so it
             * appends an error message to the $missingRequired variabl
             * similar checks are performed for other fields like 'rs_situs_state'
             * similar performans for the ottehrs fields
             */


            // Required Rules for Policy - https://jira.imprivia.xyz/browse/AW-103
        } else if (
            (
                $profileData['rs_var_coverage_employee'] ||
                $profileData['rs_var_coverage_spouse'] ||
                $profileData['rs_var_coverage-_child(ren)']
            ) &&
            $profileData['rs_policy_#_var'] === ''
        ) {
            $missingRequired .= "Missing Policy: rs_policy_#_var" . PHP_EOL;

        } else if (
            (
                $profileData['rs_ltd_class'] ||
                $profileData['rs_ltd_effective_date']
            ) &&
            $profileData['rs_policy_#_ltd'] === ''
        ) {
            $missingRequired .= "Missing Policy: rs_policy_#_ltd" . PHP_EOL;

        } else if (
            (
                $profileData['rs_vg_effective_date'] ||
                $profileData['rs_vg_class'] ||
                $profileData['rs_vg_coverage_employee'] ||
                $profileData['rs_vg_effective_date_employee'] ||
                $profileData['rs_vg_coverage_spouse'] ||
                $profileData['rs_vg_effective_date_spouse'] ||
                $profileData['rs_vg_coverage_child(ren)'] ||
                $profileData['rs_vg_effective_date_child(ren)']
            ) &&
            $profileData['rs_policy_#_vg'] === ''
        ) {
            $missingRequired .= "Missing Policy: rs_policy_#_vg" . PHP_EOL;

        } else if (
            (
                $profileData['rs_vci_class'] ||
                $profileData['rs_vci_coverage_employee'] ||
                $profileData['rs_vci_effective_date_employee'] ||
                $profileData['rs_vci_coverage_spouse'] ||
                $profileData['rs_vci_effective_date_spouse'] ||
                $profileData['rs_vci_coverage_child(ren)'] ||
                $profileData['rs_vci_effective_date_child(ren)']
            ) &&
            $profileData['rs_policy_#_vci'] === ''
        ) {
            $missingRequired .= "Missing Policy: rs_policy_#_vci" . PHP_EOL;
        } else if (
            (
                $profileData['rs_vai_class'] ||
                $profileData['rs_vai_plan'] ||
                $profileData['rs_vai_coverage_type'] ||
                $profileData['rs_vai_effective_date_employee'] ||
                $profileData['rs_vai_effective_date_spouse'] ||
                $profileData['rs_vai_effective_date_child(ren)']
            ) &&
            $profileData['rs_policy_#_vai'] === ''
        ) {
            $missingRequired .= "Missing Policy: rs_policy_#_vai" . PHP_EOL;
        } else if (
            (
                $profileData['rs_vhi_plan_option'] ||
                $profileData['rs_vhi_coverage_tier'] ||
                $profileData['rs_vhi_effective_date_employee'] ||
                $profileData['rs_vhi_effective_date_spouse'] ||
                $profileData['rs_vhi_effective_date_child(ren)']
            ) &&
            $profileData['rs_policy_#_vhi'] === ''
        ) {
            $missingRequired .= "Missing Policy: rs_policy_#_vhi" . PHP_EOL;

        } else if (
            (
                $profileData['rs_life_effective_date_of_employee'] ||
                $profileData['rs_life_effective_date_of_spouse'] ||
                $profileData['rs_life_effective_date_of_child(ren)'] ||
                $profileData['rs_basic_life_coverage_employee'] ||
                $profileData['rs_basic_ad_&_d_coverage_employee'] ||
                $profileData['rs_supplemental_ad_&_d_employee'] ||
                $profileData['rs_dep._life_coverage_spouse'] ||
                $profileData['rs_dep._life_coverage_child(ren)'] ||
                $profileData['rs_supp._life_coverage_employee'] ||
                $profileData['rs_supp._dep._coverage_spouse'] ||
                $profileData['rs_supp._dep._coverage_child(ren)']
            ) &&
            $profileData['rs_policy_#_gl'] === ''
        ) {
            $missingRequired .= "Missing Field: rs_policy_#_gl" . PHP_EOL;

        } else if (
            (
                $profileData['rs_vhi_plan_option'] &&
                $profileData['rs_vhi_coverage_tier'] &&
                $profileData['rs_vhi_effective_date_employee'] &&
                $profileData['rs_vhi_effective_date_spouse']
            ) &&
            $profileData['rs_policy_#_vhi'] === ''
        ) {
            $missingRequired .= "Missing Field: rs_policy_#_vhi" . PHP_EOL;
        }

        /*
         * checks if certains conditions related to variable coverage are met and whether the
         * conditions are met and the  field is empty, it appends an error message to  the $missingRequired
         * variable indicating that the rs_policy_#_var policy is missing
         * Similar checks are performed for other policy- realted fields
         *
         * These if and else if statements are used to validate data, check for missing information and report errors
         * or missing fields as appropiate in the context of the applications data processing logic
         */

        // VDP Data
        $name = $profileData['rs_employee_full_name'];
        $last_name = (strpos($name, ' ') === false) ? '' : preg_replace('#.*\s([\w-]*)$#', '$1', $name);
        $first_name = trim(preg_replace('#' . preg_quote($last_name, '#') . '#', '', $name));
        /*
         * extracts the firs name and last name from the employees full name it assumes that the last name comes after a space character
         */

        //assigning profile data

        $profileData['fname'] = $profileData['vdp_fname'] = $first_name;
        $profileData['lname'] = $profileData['vdp_lname'] = $last_name;
        $profileData['care_of_name'] = $profileData['vdp_fullname'] = $profileData['full_name'] = $profileData['rs_employee_full_name'];
        $profileData['address1'] = $profileData['vdp_address1'] = $profileData['rs_employee_street_address_(1)'];
        $profileData['address2'] = $profileData['vdp_address2'] = $profileData['rs_employee_street_address_(2)'];
        $profileData['city'] = $profileData['vdp_city'] = $profileData['rs_employee_city'];
        $profileData['state'] = $profileData['vdp_state'] = $profileData['rs_employee_state'];
        $profileData['zip'] = $profileData['vdp_zip'] = $profileData['rs_employee_zip'];

        /*
         * this section assigns various values from the $profileData array to specific keys
         *
         */


        $className = '01: Populated Packet';
        if (isset($this->class_name_to_id[$className])) {
            $class_id_override = $this->class_name_to_id[$className];
            $class_id_override_name = $className;
        }
        /*
        sets the class information based on the value f $className by looking up a corresponding class ID
        */

        $campaignName = 'reliance_populated'; //setting campaing name

        // Gather policy info for search
        $policySearchString = '';
        foreach ($profileData as $key => $val) {
            if (preg_match('/rs_policy_#_/', $key) && $val) {
                $policySearchString .= '|' . $val . '|';
            }
        }

        /*
         * This loop iterates through the $profileData array and collects policy information based on a specific pattern maching with rs_policy_#
         */


        //handling missing reports
        if ($missingRequired) {
            /*
             * if missing data is detected the code enters the error handling
             */
            $status = "error";
            $error = $missingRequired;

            $this->r->incr($this->rkey . 'touch_error_count');
            $this->r->incr($this->rkey . md5($this->filename) . ':touch_error_count');
            $this->records['errors'][] = [
                'data' => $data,
                'error' => $error,
                'profile_data' => $profileData,
            ];
            /*
             * if missing data is found it sets the $status to "error"  records the error message,
             * increments error counts and adds error information to the records['errors'] array
             */
        } else {

            //prcesing data with no missing data


            if ($campaignName) {

                if (isset($this->campaign_ids_by_name[$campaignName])) {
                    /*
                     * checks if valid campainName is set
                     * if a valid campaing name is found in the mapping. it sets an errror status adn records an
                     * errors message indicating that the campaing name could not be mapped to a campaing ID
                     *
                     */
                    $campaign_id = $this->campaign_ids_by_name[$campaignName];

                    //Lookup the recipient, create a new one if didn't previously exist.


                    $recipient = $this->recipientCreateOrGet([
                        'cust_id' => $this->cust_id,
                        'user_id' => $this->user_id,
                        'uni_number' => $touch_uni_number,
                        'uni_number_sub' => $touch_uni_number_sub,
                    ]);

                    //Keep track of which campaign this recipient is being added to for mass update performed later


                    if (empty($this->records['recipient_ids_by_campaign'][$campaign_id])) {
                        $this->records['recipient_ids_by_campaign'][$campaign_id] = [
                            'campaign' => ['campaign_id' => $campaign_id],
                            'recipients' => [],
                        ];
                    }
                    /*
                                       * if a valid camaping is found it retrieves the corresponding campaing_D from the campaign_ids_by_campaing
                                       *
                                       */
                    // Check for duplicate recipient ids within the same file

                    //here proceeds to hanlde the recipient related operations such as creating or getting a recipient based on certain prameters

                    $duplicate = false;
                    foreach ($this->records['recipient_ids_by_campaign'][$campaign_id]['recipients'] as $r_id) {
                        if ($r_id == $recipient->id) {
                            $duplicate = true;
                        }
                    }
                    /*
                     * it checks for duplicate recipient IDS within the same file forthe specific campaing.
                     * if a duplicate is found it sets the duplicate variable flag to true
                     */


// this condition checks if the recipient  is not duplicate
                    if (!$duplicate) {

                        $this->records['recipient_ids_by_campaign'][$campaign_id]['recipients'][] = $recipient->id;

                        //Keep track of uni_number by campaign to mass update search table to 'revision' status before adding new search data

                        //recording recipient and uni number information
                        if (empty($this->records['uni_by_campaign'][$campaign_id])) {
                            $this->records['uni_by_campaign'][$campaign_id] = [];
                        }
                        $this->records['uni_by_campaign'][$campaign_id][] = $touch_uni_number;

                        /*
                         * it records the the recipient ID in the $records['uni_by_campaign'] array
                         * it keeps track of the touch_uni_number by addingit to the $records array
                         * ths info is used for a mass updating a search table revision status later
                         */


                        //Get a new cart_id that was previously reserved from the database
                        $cart_id = $this->getCartId();
                        /*
                         * it retrieves a new cart_id that was previusly reserved from the db using the $thisgetCartID()  method
                         *
                         */

                        //Loop through campaign items and create touches
                        foreach ($this->campaigns[$campaign_id]->items as $item) {
                            /*
                             * it iterates throuch each item in the campaign and creates touch records for them
                             * inide this loop items it generates a new touch record using createTouch() method with various paramameters and data.
                             * if this the first touch_id for this recipient , it sets the $touch_id to the current touch _id
                             *
                             *
                             */
                            $class_id_override_backup = 0;
                            if (!$class_id_override) {
                                err("NO CLASS ID FOUND!!!!");
                                err(" -- Class Override Name: " . $class_id_override_name);
                                err(" -- Campaign: " . $campaignName);
                                err(" -- Item: " . $item->name);
                            }

                            //Get a new touch_id that was previously reserved from the database
                            $current_touch_id = $this->createTouch([
                                'cust_id' => $this->cust_id,
                                'user_id' => $this->user_id,
                                'item_id' => $item->id,
                                'cart_id' => $cart_id,
                                'recipient_id' => $recipient->id,
                                'campaign_id' => $campaign_id,
                                'type' => 'original',
                                'uni_number' => $touch_uni_number,
                                'class_id_override' => $class_id_override,
                                'touch_process' => $this->touch_process,
                                'status' => 'pending',
                                'global_code_revision' => $this->global_code_revision,
                                'touch_process_code_revision' => $this->touch_process_code_revision,
                                'page_count' => $pageCount,
                                'image_count' => $imageCount,
                                'import_file' => $this->filename,
                                'class_id_override_backup' => $class_id_override_backup,
                                'comments' => '',
                                'print_type' => 'user',
                                'original_date' => \Carbon\Carbon::now(),
                            ]);


                            // err("ASSIGNING TOUCH_ID: $current_touch_id");
                            // err("ASSIGNING CART_ID: $cart_id");

                            //If this is the first touch_id for this record, set it as the main touch_id
                            if ($touch_id === null) {
                                $touch_id = $current_touch_id;

                                /*
                                 * then itereates through each field of the item and creates touch data records if the corresponding data exist in the $profileData
                                 */

                                foreach ($item->fields as $field) {
                                    if (isset($profileData[$field->name])) {
                                        $touch_data_insert = [
                                            'touch_id' => $current_touch_id,
                                            'recipient_id' => $recipient->id,
                                            'profile_field_name' => $field->name,
                                            'data' => $profileData[$field->name],
                                            'edit_indicator' => 'N',
                                            'profile_link' => 'N',
                                        ];
                                        $this->createTouchData($touch_data_insert);
                                    }
                                }
                            }

                            foreach ($item->fields as $field) {
                                if (isset($profileData[$field->name])) {
                                    $touch_data_insert = [
                                        'touch_id' => $current_touch_id,
                                        'recipient_id' => $recipient->id,
                                        'profile_field_name' => $field->name,
                                        'data' => $profileData[$field->name],
                                        'edit_indicator' => 'N',
                                        'profile_link' => 'N',
                                    ];
                                    $this->createTouchData($touch_data_insert);
                                }
                            }
                        }

                        //Create the cart with previously reserved cart_id and add main touch_id

                        /*
                         * here creates a cart record using the $thiscreateCart() method with various parameters
                         */
                        $this->createCart([
                            'cart_id' => $cart_id,
                            'touch_id' => $touch_id,
                            'campaign_id' => $campaign_id,
                            'recipient_id' => $recipient->id,
                            'user_id' => $this->user_id,
                            'import_id' => $this->cust_id . '.' . $recipient->id . '.' . $touch_uni_number, //import_id
                            'status' => 'saved',
                            'touch_process' => $this->touch_process,
                            'comments' => '', //comments
                            'description' => '', //description
                        ]);

                        //$search_data prepares search data for the record and stores it in the $this records['search_data'] array with the touch_id as the key

                        $search_data = [
                            'touch_id' => $touch_id,
                            'cart_id' => $cart_id,
                            'uni_number' => $touch_uni_number,
                            'fname' => $profileData['fname'],
                            'lname' => $profileData['lname'],
                            'full_name' => $profileData['fname'] . ' ' . $profileData['lname'],
                            'address1' => $profileData['address1'],
                            'city' => $profileData['city'],
                            'state' => $profileData['state'],
                            'zip' => $profileData['zip'],
                            'policies' => $policySearchString,
                            'campaign_name' => $campaignName,
                            'campaign_id' => $campaign_id,
                            'item_ids' => '|' . $this->campaigns[$campaign_id]->items->pluck('item_id')->implode('|') . '|',
                            'type' => 'original',
                            'status' => 'pending',
                            'touch_process' => $this->touch_process,
                            'import_file' => $this->filename,
                            'date' => \Carbon\Carbon::now()->format('Y-m-d H:i:s'),
                        ];

                        $this->records['search_data'][$touch_id] = $search_data;

                        $this->r->incr($this->rkey . 'touch_count');
                        $this->r->incr($this->rkey . md5($this->filename) . ':touch_count');
                        $this->r->rPush($this->rkey . 'touches_processed', $touch_id);
                        $success = true;
                        /*
                         * this part is responsible for creating touch records, handling touch data, creating cart records and recording search data for a recipient within a
                         * specific campaing. it ensures that data is properly organized and processed for futher use
                         *
                         * here increments counter related to the number of touches proccessed, both globally and based on the file name
                         * it pushes the touch_id to a list processed touches
                         * Finally it sets $success to true  indicating that the processing for this recipient was successfull
                         *
                         *
                         */

                    } else {

                        /*
                         * DUPLICATES STATEMENTS
                         * the following else staments handle different error scenarios ehn processing data for a
                         * recipient in the context of a specific campaign
                         */


                        $this->r->incr($this->rkey . 'touch_error_count');
                        $this->r->incr($this->rkey . md5($this->filename) . ':touch_error_count');

                        $this->r->incr($this->rkey . 'duplicate_count');
                        $this->r->incr($this->rkey . md5($this->filename) . ':duplicate_count');

                        $status = 'duplicate';
                        $error = 'Duplicate record - UNI:' . $touch_uni_number . ' UNI-SUB:' . $touch_uni_number_sub;
                        $this->records['errors'][] = [
                            'data' => $data,
                            'error' => $error,
                            'profile_data' => $profileData,
                        ];

                        // Remove prior duplicate record
                        // $this->records['recipient_ids_by_campaign'][$campaign_id]
                        // $this->records['uni_by_campaign'][$campaign_id]
                        // $this->records['search_data'][$touch_id]

                        /*
                         * here handles the scenario where a duplicata recipient is encountered
                         * it increments counters related to touch error counts and duplicates counts
                         * sets the 'status'  variable to duplicate to indicate that this is a duplicate record
                         * generates an error message indicating the duplication with details of UNI and UNI-SUB
                         * it adds an error entry to the $records['error'] array containing data error, message and profile data
                         * NOTE: it is commented out code for removing prior duplicate records although its currently commented and may not be active
                         */
                    }
                } else {
                    $this->r->incr($this->rkey . 'touch_error_count');
                    $this->r->incr($this->rkey . md5($this->filename) . ':touch_error_count');

                    $status = 'error';
                    $error = 'Cannot map campaign name to campaign_id campaign_name = ' . $campaignName;
                    $this->records['errors'][] = [
                        'data' => $data,
                        'error' => $error,
                        'profile_data' => $profileData,
                    ];

                }

                /*
                 * handles the scenario where it cannot map the campaing name to a campaing ID
                similar to the previous it incrementes counters relaated totouch error counts
                sets the status variable to error to indicate an error condition
                generates an error message indicating that it cannot map the campaing name to a campaign ID including the campaing_name

                adds an error to the array containingdata error, message and profile data
                 */


            } else {
                $this->r->incr($this->rkey . 'touch_error_count');
                $this->r->incr($this->rkey . md5($this->filename) . ':touch_error_count');

                $status = 'error';
                $error = 'Cannot find campaign name';
                $this->records['errors'][] = [
                    'data' => $data,
                    'error' => $error,
                    'profile_data' => $profileData,
                ];

            }/*
                              * her handles the scenario where it cannot find the campaing name at all
                              * it increments counters related to touch error counts
                              * sets the status variable to error to indicate an error condition
                              * generates an error message indicating that it cannot find the campaing name
                              * adds an error entry to the $records['errors'] array containing data , error message and profile data
                              */
        }

        /*
         * this section creates an import log entry containing various details about the import process and appends it to the  import_logData array
         */

        $import_log_data = [
            'touch_id' => $touch_id ?? null,
            'filename' => $this->filename,
            'campaign_name' => $campaignName ?? null,
            'campaign_id' => $campaign_id ?? null,
            'touch_uni_number' => $touch_uni_number,
            'uni_number' => $uni_number,
            'fname' => $profileData['fname'],
            'lname' => $profileData['lname'],
            'address' => $profileData['address1'],
            'city' => $profileData['city'],
            'state' => $profileData['state'],
            'zip' => $profileData['zip'],
            'touch_process' => $this->touch_process,
            'raw_data' => serialize($profileData),
            'error' => $error,
            'status' => $status,
            'info' => $qleNotMatchedInfoText
        ];

        $this->records['import_logs'][] = $import_log_data;
        $this->r->rPush($this->rkey . 'touches_processed', $import_log_data['touch_id']);

        /*
         * this array $import_log_data is created, which contains various ppieces of information related to the import proceess. This information includes details about the imported data,
         * campaing, recipient, any errorsencounted and more
         * The import_logs array is appended to the This records['import_logs'] array. This is a log of import related data, where each entryin the import log array corresponds to a single import operation
         * The touch_id from the import log data is pushed into a Redis list using $this->r->rPush(). this is a method for storting information about pocessed touches or imports
         * Finally the function return the value of $sucess , the value likely indicates whether the entire import process was successful. Dependindg on the earlier logic in the code, this variable
         * may be set to 'true' if the inmport was successfull  and false if it there were errors.
         */

        return $success;
    }
}
