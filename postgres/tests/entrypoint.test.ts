import {test,expect} from 'bun:test';
test('driver-neutral entry point does not import optional connection factories',async()=>{
 const result=await Bun.build({entrypoints:[new URL('../src/drizzle.ts',import.meta.url).pathname],target:'bun',external:['@absolutejs/queue','drizzle-orm'],plugins:[{name:'reject-optional-drivers',setup(build){build.onResolve({filter:/^(postgres|@neondatabase\/serverless)$/},()=>{throw Error('Optional driver must not be imported');});}}]});expect(result.success).toBe(true);
});
