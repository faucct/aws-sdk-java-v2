/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.services.s3.checksums;

import static software.amazon.awssdk.utils.FunctionalUtils.invokeSafely;

import java.io.InputStream;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.utils.IoUtils;

public class GetObjectAttributesSizeInterceptor implements ExecutionInterceptor {

    private Long objectSize ;

    public Long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(Long objectSize) {
        this.objectSize = objectSize;
    }

    public  String attributeValue(String input, String attributeName) {
        String start = String.format("<%s>", attributeName);
        String end = String.format("</%s>", attributeName);
        Pattern pattern = Pattern.compile(Pattern.quote(start) + "(.*?)" + Pattern.quote(end));
        Matcher matcher = pattern.matcher(input);
        return matcher.find() ? matcher.group(1) : null;
    }
    public String replaceAttribute(String input, String attributeName, String replacement) {
        String start = String.format("<%s>", attributeName);
        String end = String.format("</%s>", attributeName);
        Pattern pattern = Pattern.compile(Pattern.quote(start) + "(.*?)" + Pattern.quote(end));
        Matcher matcher = pattern.matcher(input);
        return matcher.replaceAll(Matcher.quoteReplacement(replacement));
    }

    @Override
    public Optional<InputStream> modifyHttpResponseContent(Context.ModifyHttpResponse context,
                                                           ExecutionAttributes executionAttributes) {



        // Interceptors checked exclusively for GetObjectAttributesRequest
        if(context.request() instanceof GetObjectAttributesRequest){
            GetObjectAttributesRequest getObjectAttributesRequest = (GetObjectAttributesRequest) context.request();

            // Resetting the object size before
            objectSize = -1L;

            // Interceptors modifies the content only for ObjectAttributes of OBJECT_SIZE
            if(getObjectAttributesRequest.hasObjectAttributes() &&
               getObjectAttributesRequest.objectAttributes().stream().anyMatch(o -> o.equals(ObjectAttributes.OBJECT_SIZE))){
                Optional<String> contentOptional = context.responseBody().map(r -> invokeSafely(() -> IoUtils.toUtf8String(r)));
                if (contentOptional.isPresent()) {
                    String stringBetween = attributeValue(contentOptional.get(), "ObjectSize");
                    try{
                        objectSize = Long.parseLong(stringBetween);
                        Integer.parseInt(stringBetween);
                    }catch (NumberFormatException stateException){
                        String replaceStringBetween = replaceAttribute(contentOptional.get(),
                                                                       "ObjectSize",
                                                                       "null");

                        return Optional.of(RequestBody.fromString(replaceStringBetween).contentStreamProvider().newStream());
                    }
                    return Optional.of(RequestBody.fromString(contentOptional.get()).contentStreamProvider().newStream());
                }
            }
        }
        return ExecutionInterceptor.super.modifyHttpResponseContent(context, executionAttributes);
    }
}

