var Xx=Object.defineProperty,Yx=Object.defineProperties;var Zx=Object.getOwnPropertyDescriptors;var Vm=Object.getOwnPropertySymbols;var Jx=Object.prototype.hasOwnProperty,Kx=Object.prototype.propertyIsEnumerable;var Bm=(n,e,t)=>e in n?Xx(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,dt=(n,e)=>{for(var t in e||={})Jx.call(e,t)&&Bm(n,t,e[t]);if(Vm)for(var t of Vm(e))Kx.call(e,t)&&Bm(n,t,e[t]);return n},wt=(n,e)=>Yx(n,Zx(e));var ds=(n,e,t)=>new Promise((i,r)=>{var s=c=>{try{a(t.next(c))}catch(l){r(l)}},o=c=>{try{a(t.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(s,o);a((t=t.apply(n,e)).next())});function Fu(n,e){return Object.is(n,e)}var Bt=null,Pa=!1,Lu=1,Hn=Symbol("SIGNAL");function Ze(n){let e=Bt;return Bt=n,e}function ku(){return Bt}var Mo={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Fa(n){if(Pa)throw new Error("");if(Bt===null)return;Bt.consumerOnSignalRead(n);let e=Bt.nextProducerIndex++;if(Ua(Bt),e<Bt.producerNode.length&&Bt.producerNode[e]!==n&&bo(Bt)){let t=Bt.producerNode[e];ka(t,Bt.producerIndexOfThis[e])}Bt.producerNode[e]!==n&&(Bt.producerNode[e]=n,Bt.producerIndexOfThis[e]=bo(Bt)?zm(n,Bt,e):0),Bt.producerLastReadVersion[e]=n.version}function Hm(){Lu++}function Uu(n){if(!(bo(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Lu)){if(!n.producerMustRecompute(n)&&!zu(n)){Ou(n);return}n.producerRecomputeValue(n),Ou(n)}}function Vu(n){if(n.liveConsumerNode===void 0)return;let e=Pa;Pa=!0;try{for(let t of n.liveConsumerNode)t.dirty||Qx(t)}finally{Pa=e}}function Bu(){return Bt?.consumerAllowSignalWrites!==!1}function Qx(n){n.dirty=!0,Vu(n),n.consumerMarkedDirty?.(n)}function Ou(n){n.dirty=!1,n.lastCleanEpoch=Lu}function La(n){return n&&(n.nextProducerIndex=0),Ze(n)}function Hu(n,e){if(Ze(e),!(!n||n.producerNode===void 0||n.producerIndexOfThis===void 0||n.producerLastReadVersion===void 0)){if(bo(n))for(let t=n.nextProducerIndex;t<n.producerNode.length;t++)ka(n.producerNode[t],n.producerIndexOfThis[t]);for(;n.producerNode.length>n.nextProducerIndex;)n.producerNode.pop(),n.producerLastReadVersion.pop(),n.producerIndexOfThis.pop()}}function zu(n){Ua(n);for(let e=0;e<n.producerNode.length;e++){let t=n.producerNode[e],i=n.producerLastReadVersion[e];if(i!==t.version||(Uu(t),i!==t.version))return!0}return!1}function Gu(n){if(Ua(n),bo(n))for(let e=0;e<n.producerNode.length;e++)ka(n.producerNode[e],n.producerIndexOfThis[e]);n.producerNode.length=n.producerLastReadVersion.length=n.producerIndexOfThis.length=0,n.liveConsumerNode&&(n.liveConsumerNode.length=n.liveConsumerIndexOfThis.length=0)}function zm(n,e,t){if(Gm(n),n.liveConsumerNode.length===0&&Wm(n))for(let i=0;i<n.producerNode.length;i++)n.producerIndexOfThis[i]=zm(n.producerNode[i],n,i);return n.liveConsumerIndexOfThis.push(t),n.liveConsumerNode.push(e)-1}function ka(n,e){if(Gm(n),n.liveConsumerNode.length===1&&Wm(n))for(let i=0;i<n.producerNode.length;i++)ka(n.producerNode[i],n.producerIndexOfThis[i]);let t=n.liveConsumerNode.length-1;if(n.liveConsumerNode[e]=n.liveConsumerNode[t],n.liveConsumerIndexOfThis[e]=n.liveConsumerIndexOfThis[t],n.liveConsumerNode.length--,n.liveConsumerIndexOfThis.length--,e<n.liveConsumerNode.length){let i=n.liveConsumerIndexOfThis[e],r=n.liveConsumerNode[e];Ua(r),r.producerIndexOfThis[i]=e}}function bo(n){return n.consumerIsAlwaysLive||(n?.liveConsumerNode?.length??0)>0}function Ua(n){n.producerNode??=[],n.producerIndexOfThis??=[],n.producerLastReadVersion??=[]}function Gm(n){n.liveConsumerNode??=[],n.liveConsumerIndexOfThis??=[]}function Wm(n){return n.producerNode!==void 0}function Wu(n,e){let t=Object.create(eb);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(Uu(t),Fa(t),t.value===Oa)throw t.error;return t.value};return i[Hn]=t,i}var Nu=Symbol("UNSET"),Pu=Symbol("COMPUTING"),Oa=Symbol("ERRORED"),eb=wt(dt({},Mo),{value:Nu,dirty:!0,error:null,equal:Fu,kind:"computed",producerMustRecompute(n){return n.value===Nu||n.value===Pu},producerRecomputeValue(n){if(n.value===Pu)throw new Error("Detected cycle in computations.");let e=n.value;n.value=Pu;let t=La(n),i,r=!1;try{i=n.computation(),Ze(null),r=e!==Nu&&e!==Oa&&i!==Oa&&n.equal(e,i)}catch(s){i=Oa,n.error=s}finally{Hu(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function tb(){throw new Error}var jm=tb;function $m(n){jm(n)}function ju(n){jm=n}var nb=null;function $u(n,e){let t=Object.create(Xu);t.value=n,e!==void 0&&(t.equal=e);let i=()=>(Fa(t),t.value);return i[Hn]=t,i}function Va(n,e){Bu()||$m(n),n.equal(n.value,e)||(n.value=e,ib(n))}function qu(n,e){Bu()||$m(n),Va(n,e(n.value))}var Xu=wt(dt({},Mo),{equal:Fu,value:void 0,kind:"signal"});function ib(n){n.version++,Hm(),Vu(n),nb?.()}function Yu(n){let e=Ze(null);try{return n()}finally{Ze(e)}}var Zu;function Eo(){return Zu}function hi(n){let e=Zu;return Zu=n,e}var Ba=Symbol("NotFound");function Je(n){return typeof n=="function"}function Ha(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var za=Ha(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function So(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var rn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Je(i))try{i()}catch(s){e=s instanceof za?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{qm(s)}catch(o){e=e??[],o instanceof za?e=[...e,...o.errors]:e.push(o)}}if(e)throw new za(e)}}add(e){var t;if(e&&e!==this)if(this.closed)qm(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&So(t,e)}remove(e){let{_finalizers:t}=this;t&&So(t,e),e instanceof n&&e._removeParent(this)}};rn.EMPTY=(()=>{let n=new rn;return n.closed=!0,n})();var Ju=rn.EMPTY;function Ga(n){return n instanceof rn||n&&"closed"in n&&Je(n.remove)&&Je(n.add)&&Je(n.unsubscribe)}function qm(n){Je(n)?n():n.unsubscribe()}var zn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var fs={setTimeout(n,e,...t){let{delegate:i}=fs;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=fs;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Wa(n){fs.setTimeout(()=>{let{onUnhandledError:e}=zn;if(e)e(n);else throw n})}function Ku(){}var Xm=Qu("C",void 0,void 0);function Ym(n){return Qu("E",void 0,n)}function Zm(n){return Qu("N",n,void 0)}function Qu(n,e,t){return{kind:n,value:e,error:t}}var xr=null;function hs(n){if(zn.useDeprecatedSynchronousErrorHandling){let e=!xr;if(e&&(xr={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=xr;if(xr=null,t)throw i}}else n()}function Jm(n){zn.useDeprecatedSynchronousErrorHandling&&xr&&(xr.errorThrown=!0,xr.error=n)}var br=class extends rn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Ga(e)&&e.add(this)):this.destination=lb}static create(e,t,i){return new ps(e,t,i)}next(e){this.isStopped?td(Zm(e),this):this._next(e)}error(e){this.isStopped?td(Ym(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?td(Xm,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},ab=Function.prototype.bind;function ed(n,e){return ab.call(n,e)}var nd=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){ja(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){ja(i)}else ja(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){ja(t)}}},ps=class extends br{constructor(e,t,i){super();let r;if(Je(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&zn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&ed(e.next,s),error:e.error&&ed(e.error,s),complete:e.complete&&ed(e.complete,s)}):r=e}this.destination=new nd(r)}};function ja(n){zn.useDeprecatedSynchronousErrorHandling?Jm(n):Wa(n)}function cb(n){throw n}function td(n,e){let{onStoppedNotification:t}=zn;t&&fs.setTimeout(()=>t(n,e))}var lb={closed:!0,next:Ku,error:cb,complete:Ku};var ms=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Km(n){return n}function Qm(n){return n.length===0?Km:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Pt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=db(t)?t:new ps(t,i,r);return hs(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=eg(i),new i((r,s)=>{let o=new ps({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[ms](){return this}pipe(...t){return Qm(t)(this)}toPromise(t){return t=eg(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function eg(n){var e;return(e=n??zn.Promise)!==null&&e!==void 0?e:Promise}function ub(n){return n&&Je(n.next)&&Je(n.error)&&Je(n.complete)}function db(n){return n&&n instanceof br||ub(n)&&Ga(n)}function fb(n){return Je(n?.lift)}function ln(n){return e=>{if(fb(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function un(n,e,t,i,r){return new id(n,e,t,i,r)}var id=class extends br{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var tg=Ha(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var dn=(()=>{class n extends Pt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new $a(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new tg}next(t){hs(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){hs(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){hs(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?Ju:(this.currentObservers=null,s.push(t),new rn(()=>{this.currentObservers=null,So(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Pt;return t.source=this,t}}return n.create=(e,t)=>new $a(e,t),n})(),$a=class extends dn{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Ju}};var pi=class extends dn{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function ng(n){return n&&Je(n.schedule)}function ig(n){return n[n.length-1]}function rg(n){return Je(ig(n))?n.pop():void 0}function sg(n){return ng(ig(n))?n.pop():void 0}function ag(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(d){o(d)}}function c(u){try{l(i.throw(u))}catch(d){o(d)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function og(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Mr(n){return this instanceof Mr?(this.v=n,this):new Mr(n)}function cg(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(f){return function(g){return Promise.resolve(g).then(f,d)}}function a(f,g){i[f]&&(r[f]=function(v){return new Promise(function(m,p){s.push([f,v,m,p])>1||c(f,v)})},g&&(r[f]=g(r[f])))}function c(f,g){try{l(i[f](g))}catch(v){h(s[0][3],v)}}function l(f){f.value instanceof Mr?Promise.resolve(f.value.v).then(u,d):h(s[0][2],f)}function u(f){c("next",f)}function d(f){c("throw",f)}function h(f,g){f(g),s.shift(),s.length&&c(s[0][0],s[0][1])}}function lg(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof og=="function"?og(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var qa=n=>n&&typeof n.length=="number"&&typeof n!="function";function Xa(n){return Je(n?.then)}function Ya(n){return Je(n[ms])}function Za(n){return Symbol.asyncIterator&&Je(n?.[Symbol.asyncIterator])}function Ja(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function hb(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Ka=hb();function Qa(n){return Je(n?.[Ka])}function ec(n){return cg(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield Mr(t.read());if(r)return yield Mr(void 0);yield yield Mr(i)}}finally{t.releaseLock()}})}function tc(n){return Je(n?.getReader)}function fn(n){if(n instanceof Pt)return n;if(n!=null){if(Ya(n))return pb(n);if(qa(n))return mb(n);if(Xa(n))return gb(n);if(Za(n))return ug(n);if(Qa(n))return vb(n);if(tc(n))return yb(n)}throw Ja(n)}function pb(n){return new Pt(e=>{let t=n[ms]();if(Je(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function mb(n){return new Pt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function gb(n){return new Pt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,Wa)})}function vb(n){return new Pt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function ug(n){return new Pt(e=>{_b(n,e).catch(t=>e.error(t))})}function yb(n){return ug(ec(n))}function _b(n,e){var t,i,r,s;return ag(this,void 0,void 0,function*(){try{for(t=lg(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function Dn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function nc(n,e=0){return ln((t,i)=>{t.subscribe(un(i,r=>Dn(i,n,()=>i.next(r),e),()=>Dn(i,n,()=>i.complete(),e),r=>Dn(i,n,()=>i.error(r),e)))})}function ic(n,e=0){return ln((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function dg(n,e){return fn(n).pipe(ic(e),nc(e))}function fg(n,e){return fn(n).pipe(ic(e),nc(e))}function hg(n,e){return new Pt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function pg(n,e){return new Pt(t=>{let i;return Dn(t,e,()=>{i=n[Ka](),Dn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>Je(i?.return)&&i.return()})}function rc(n,e){if(!n)throw new Error("Iterable cannot be null");return new Pt(t=>{Dn(t,e,()=>{let i=n[Symbol.asyncIterator]();Dn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function mg(n,e){return rc(ec(n),e)}function gg(n,e){if(n!=null){if(Ya(n))return dg(n,e);if(qa(n))return hg(n,e);if(Xa(n))return fg(n,e);if(Za(n))return rc(n,e);if(Qa(n))return pg(n,e);if(tc(n))return mg(n,e)}throw Ja(n)}function Er(n,e){return e?gg(n,e):fn(n)}function sc(...n){let e=sg(n);return Er(n,e)}function hn(n,e){return ln((t,i)=>{let r=0;t.subscribe(un(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:xb}=Array;function bb(n,e){return xb(e)?n(...e):n(e)}function vg(n){return hn(e=>bb(n,e))}var{isArray:Mb}=Array,{getPrototypeOf:Eb,prototype:Sb,keys:wb}=Object;function yg(n){if(n.length===1){let e=n[0];if(Mb(e))return{args:e,keys:null};if(Cb(e)){let t=wb(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Cb(n){return n&&typeof n=="object"&&Eb(n)===Sb}function _g(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function xg(n,e,t,i,r,s,o,a){let c=[],l=0,u=0,d=!1,h=()=>{d&&!c.length&&!l&&e.complete()},f=v=>l<i?g(v):c.push(v),g=v=>{s&&e.next(v),l++;let m=!1;fn(t(v,u++)).subscribe(un(e,p=>{r?.(p),s?f(p):e.next(p)},()=>{m=!0},void 0,()=>{if(m)try{for(l--;c.length&&l<i;){let p=c.shift();o?Dn(e,o,()=>g(p)):g(p)}h()}catch(p){e.error(p)}}))};return n.subscribe(un(e,f,()=>{d=!0,h()})),()=>{a?.()}}function oc(n,e,t=1/0){return Je(e)?oc((i,r)=>hn((s,o)=>e(i,s,r,o))(fn(n(i,r))),t):(typeof e=="number"&&(t=e),ln((i,r)=>xg(i,r,n,t)))}function rd(...n){let e=rg(n),{args:t,keys:i}=yg(n),r=new Pt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),c=o,l=o;for(let u=0;u<o;u++){let d=!1;fn(t[u]).subscribe(un(s,h=>{d||(d=!0,l--),a[u]=h},()=>c--,void 0,()=>{(!c||!d)&&(l||s.next(i?_g(i,a):a),s.complete())}))}});return e?r.pipe(vg(e)):r}function sd(n,e){return ln((t,i)=>{let r=0;t.subscribe(un(i,s=>n.call(e,s,r++)&&i.next(s)))})}function od(n,e){return Je(e)?oc(n,e,1):oc(n,1)}function ad(n){return ln((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function cd(n,e){return ln((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(un(i,c=>{r?.unsubscribe();let l=0,u=s++;fn(n(c,u)).subscribe(r=un(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{o=!0,a()}))})}var av="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Te=class extends Error{code;constructor(e,t){super(Db(e,t)),this.code=e}};function Tb(n){return`NG0${Math.abs(n)}`}function Db(n,e){return`${Tb(n)}${e?": "+e:""}`}function Vo(n){return{toString:n}.toString()}var ac="__parameters__";function Ab(n){return function(...t){if(n){let i=n(...t);for(let r in i)this[r]=i[r]}}}function cv(n,e,t){return Vo(()=>{let i=Ab(e);function r(...s){if(this instanceof r)return i.apply(this,s),this;let o=new r(...s);return a.annotation=o,a;function a(c,l,u){let d=c.hasOwnProperty(ac)?c[ac]:Object.defineProperty(c,ac,{value:[]})[ac];for(;d.length<=u;)d.push(null);return(d[u]=d[u]||[]).push(o),c}}return r.prototype.ngMetadataName=n,r.annotationCls=r,r})}function Et(n){for(let e in n)if(n[e]===Et)return e;throw Error("Could not find renamed property on target object.")}function Ib(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function In(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(In).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function bg(n,e){return n?e?`${n} ${e}`:n:e||""}var Rb=Et({__forward_ref__:Et});function Lr(n){return n.__forward_ref__=Lr,n.toString=function(){return In(this())},n}function sn(n){return lv(n)?n():n}function lv(n){return typeof n=="function"&&n.hasOwnProperty(Rb)&&n.__forward_ref__===Lr}function tt(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function kr(n){return{providers:n.providers||[],imports:n.imports||[]}}function mf(n){return Mg(n,uv)||Mg(n,dv)}function Mg(n,e){return n.hasOwnProperty(e)?n[e]:null}function Nb(n){let e=n&&(n[uv]||n[dv]);return e||null}function Eg(n){return n&&(n.hasOwnProperty(Sg)||n.hasOwnProperty(Pb))?n[Sg]:null}var uv=Et({\u0275prov:Et}),Sg=Et({\u0275inj:Et}),dv=Et({ngInjectableDef:Et}),Pb=Et({ngInjectorDef:Et}),Ne=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=tt({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function fv(n){return n&&!!n.\u0275providers}var Ob=Et({\u0275cmp:Et}),Fb=Et({\u0275dir:Et}),Lb=Et({\u0275pipe:Et});var mc=Et({\u0275fac:Et}),Do=Et({__NG_ELEMENT_ID__:Et}),wg=Et({__NG_ENV_ID__:Et});function Ao(n){return typeof n=="string"?n:n==null?"":String(n)}function kb(n){return typeof n=="function"?n.name||n.toString():typeof n=="object"&&n!=null&&typeof n.type=="function"?n.type.name||n.type.toString():Ao(n)}function hv(n,e){throw new Te(-200,n)}function gf(n,e){throw new Te(-201,!1)}var We=(function(n){return n[n.Default=0]="Default",n[n.Host=1]="Host",n[n.Self=2]="Self",n[n.SkipSelf=4]="SkipSelf",n[n.Optional=8]="Optional",n})(We||{}),Ed;function pv(){return Ed}function pn(n){let e=Ed;return Ed=n,e}function mv(n,e,t){let i=mf(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&We.Optional)return null;if(e!==void 0)return e;gf(n,"Injector")}var Ub={},Sr=Ub,Sd="__NG_DI_FLAG__",gc=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=t;return this.injector.get(e,i.optional?Ba:Sr,i)}},vc="ngTempTokenPath",Vb="ngTokenPath",Bb=/\n/gm,Hb="\u0275",Cg="__source";function zb(n,e=We.Default){if(Eo()===void 0)throw new Te(-203,!1);if(Eo()===null)return mv(n,void 0,e);{let t=Eo(),i;return t instanceof gc?i=t.injector:i=t,i.get(n,e&We.Optional?null:void 0,e)}}function Xe(n,e=We.Default){return(pv()||zb)(sn(n),e)}function qe(n,e=We.Default){return Xe(n,zc(e))}function zc(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function wd(n){let e=[];for(let t=0;t<n.length;t++){let i=sn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Te(900,!1);let r,s=We.Default;for(let o=0;o<i.length;o++){let a=i[o],c=Gb(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(Xe(r,s))}else e.push(Xe(i))}return e}function gv(n,e){return n[Sd]=e,n.prototype[Sd]=e,n}function Gb(n){return n[Sd]}function Wb(n,e,t,i){let r=n[vc];throw e[Cg]&&r.unshift(e[Cg]),n.message=jb(`
`+n.message,r,t,i),n[Vb]=r,n[vc]=null,n}function jb(n,e,t,i=null){n=n&&n.charAt(0)===`
`&&n.charAt(1)==Hb?n.slice(2):n;let r=In(e);if(Array.isArray(e))r=e.map(In).join(" -> ");else if(typeof e=="object"){let s=[];for(let o in e)if(e.hasOwnProperty(o)){let a=e[o];s.push(o+":"+(typeof a=="string"?JSON.stringify(a):In(a)))}r=`{${s.join(", ")}}`}return`${t}${i?"("+i+")":""}[${r}]: ${n.replace(Bb,`
  `)}`}var $b=gv(cv("Optional"),8);var qb=gv(cv("SkipSelf"),4);function Cr(n,e){let t=n.hasOwnProperty(mc);return t?n[mc]:null}function Xb(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function Yb(n){return n.flat(Number.POSITIVE_INFINITY)}function vf(n,e){n.forEach(t=>Array.isArray(t)?vf(t,e):e(t))}function vv(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function yc(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function Zb(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function Jb(n,e,t){let i=Bo(n,e);return i>=0?n[i|1]=t:(i=~i,Zb(n,i,e,t)),i}function ld(n,e){let t=Bo(n,e);if(t>=0)return n[t|1]}function Bo(n,e){return Kb(n,e,1)}function Kb(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var Tr={},An=[],_c=new Ne(""),yv=new Ne("",-1),_v=new Ne(""),xc=class{get(e,t=Sr){if(t===Sr){let i=new Error(`NullInjectorError: No provider for ${In(e)}!`);throw i.name="NullInjectorError",i}return t}};function Io(n){return n[Ob]||null}function Qb(n){return n[Fb]||null}function eM(n){return n[Lb]||null}function xv(n){return{\u0275providers:n}}function tM(...n){return{\u0275providers:bv(!0,n),\u0275fromNgModule:!0}}function bv(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return vf(e,o=>{let a=o;Cd(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Mv(r,s),t}function Mv(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];yf(r,s=>{e(s,i)})}}function Cd(n,e,t,i){if(n=sn(n),!n)return!1;let r=null,s=Eg(n),o=!s&&Io(n);if(!s&&!o){let c=n.ngModule;if(s=Eg(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)Cd(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;vf(s.imports,u=>{Cd(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&Mv(l,e)}if(!a){let l=Cr(r)||(()=>new r);e({provide:r,useFactory:l,deps:An},r),e({provide:_v,useValue:r,multi:!0},r),e({provide:_c,useValue:()=>Xe(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;yf(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function yf(n,e){for(let t of n)fv(t)&&(t=t.\u0275providers),Array.isArray(t)?yf(t,e):e(t)}var nM=Et({provide:String,useValue:Et});function Ev(n){return n!==null&&typeof n=="object"&&nM in n}function iM(n){return!!(n&&n.useExisting)}function rM(n){return!!(n&&n.useFactory)}function xs(n){return typeof n=="function"}function sM(n){return!!n.useClass}var Gc=new Ne(""),lc={},Tg={},ud;function _f(){return ud===void 0&&(ud=new xc),ud}var ei=class{},Ro=class extends ei{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Dd(e,o=>this.processProvider(o)),this.records.set(yv,gs(void 0,this)),r.has("environment")&&this.records.set(ei,gs(void 0,this));let s=this.records.get(Gc);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(_v,An,We.Self))}retrieve(e,t){let i=t;return this.get(e,i.optional?Ba:Sr,i)}destroy(){Co(this),this._destroyed=!0;let e=Ze(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ze(e)}}onDestroy(e){return Co(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Co(this);let t=hi(this),i=pn(void 0),r;try{return e()}finally{hi(t),pn(i)}}get(e,t=Sr,i=We.Default){if(Co(this),e.hasOwnProperty(wg))return e[wg](this);i=zc(i);let r,s=hi(this),o=pn(void 0);try{if(!(i&We.SkipSelf)){let c=this.records.get(e);if(c===void 0){let l=uM(e)&&mf(e);l&&this.injectableDefInScope(l)?c=gs(Td(e),lc):c=null,this.records.set(e,c)}if(c!=null)return this.hydrate(e,c,i)}let a=i&We.Self?_f():this.parent;return t=i&We.Optional&&t===Sr?null:t,a.get(e,t)}catch(a){if(a.name==="NullInjectorError"){if((a[vc]=a[vc]||[]).unshift(In(e)),s)throw a;return Wb(a,e,"R3InjectorError",this.source)}else throw a}finally{pn(o),hi(s)}}resolveInjectorInitializers(){let e=Ze(null),t=hi(this),i=pn(void 0),r;try{let s=this.get(_c,An,We.Self);for(let o of s)o()}finally{hi(t),pn(i),Ze(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(In(i));return`R3Injector[${e.join(", ")}]`}processProvider(e){e=sn(e);let t=xs(e)?e:sn(e&&e.provide),i=aM(e);if(!xs(e)&&e.multi===!0){let r=this.records.get(t);r||(r=gs(void 0,lc,!0),r.factory=()=>wd(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Ze(null);try{return t.value===Tg?hv(In(e)):t.value===lc&&(t.value=Tg,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&lM(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ze(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=sn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Td(n){let e=mf(n),t=e!==null?e.factory:Cr(n);if(t!==null)return t;if(n instanceof Ne)throw new Te(204,!1);if(n instanceof Function)return oM(n);throw new Te(204,!1)}function oM(n){if(n.length>0)throw new Te(204,!1);let t=Nb(n);return t!==null?()=>t.factory(n):()=>new n}function aM(n){if(Ev(n))return gs(void 0,n.useValue);{let e=Sv(n);return gs(e,lc)}}function Sv(n,e,t){let i;if(xs(n)){let r=sn(n);return Cr(r)||Td(r)}else if(Ev(n))i=()=>sn(n.useValue);else if(rM(n))i=()=>n.useFactory(...wd(n.deps||[]));else if(iM(n))i=(r,s)=>Xe(sn(n.useExisting),s!==void 0&&s&We.Optional?We.Optional:void 0);else{let r=sn(n&&(n.useClass||n.provide));if(cM(n))i=()=>new r(...wd(n.deps));else return Cr(r)||Td(r)}return i}function Co(n){if(n.destroyed)throw new Te(205,!1)}function gs(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function cM(n){return!!n.deps}function lM(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function uM(n){return typeof n=="function"||typeof n=="object"&&n instanceof Ne}function Dd(n,e){for(let t of n)Array.isArray(t)?Dd(t,e):t&&fv(t)?Dd(t.\u0275providers,e):e(t)}function Wc(n,e){let t;n instanceof Ro?(Co(n),t=n):t=new gc(n);let i,r=hi(t),s=pn(void 0);try{return e()}finally{hi(r),pn(s)}}function dM(){return pv()!==void 0||Eo()!=null}function fM(n){return typeof n=="function"}var _i=0,Ge=1,Fe=2,Kt=3,Wn=4,jn=5,bc=6,Mc=7,mn=8,bs=9,mi=10,Gt=11,No=12,Dg=13,Cs=14,ti=15,Dr=16,vs=17,gi=18,jc=19,wv=20,Xi=21,dd=22,Ec=23,Rn=24,fd=25,Nn=26,Cv=1;var Ar=7,Sc=8,Ms=9,Mn=10;function Yi(n){return Array.isArray(n)&&typeof n[Cv]=="object"}function xi(n){return Array.isArray(n)&&n[Cv]===!0}function Tv(n){return(n.flags&4)!==0}function Ts(n){return n.componentOffset>-1}function xf(n){return(n.flags&1)===1}function ni(n){return!!n.template}function wc(n){return(n[Fe]&512)!==0}function Ds(n){return(n[Fe]&256)===256}var Ad=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function Dv(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var bf=(()=>{let n=()=>Av;return n.ngInherit=!0,n})();function Av(n){return n.type.prototype.ngOnChanges&&(n.setInput=pM),hM}function hM(){let n=Rv(this),e=n?.current;if(e){let t=n.previous;if(t===Tr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function pM(n,e,t,i,r){let s=this.declaredInputs[i],o=Rv(n)||mM(n,{previous:Tr,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new Ad(l&&l.currentValue,t,c===Tr),Dv(n,e,r,t)}var Iv="__ngSimpleChanges__";function Rv(n){return n[Iv]||null}function mM(n,e){return n[Iv]=e}var Ag=null;var yt=function(n,e=null,t){Ag?.(n,e,t)},gM="svg",vM="math";function ii(n){for(;Array.isArray(n);)n=n[_i];return n}function Nv(n,e){return ii(e[n])}function bi(n,e){return ii(e[n.index])}function Pv(n,e){return n.data[e]}function Ov(n,e){return n[e]}function yM(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function ri(n,e){let t=e[n];return Yi(t)?t:t[_i]}function _M(n){return(n[Fe]&4)===4}function Mf(n){return(n[Fe]&128)===128}function xM(n){return xi(n[Kt])}function Cc(n,e){return e==null?null:n[e]}function Fv(n){n[vs]=0}function Lv(n){n[Fe]&1024||(n[Fe]|=1024,Mf(n)&&qc(n))}function bM(n,e){for(;n>0;)e=e[Cs],n--;return e}function $c(n){return!!(n[Fe]&9216||n[Rn]?.dirty)}function Id(n){n[mi].changeDetectionScheduler?.notify(8),n[Fe]&64&&(n[Fe]|=1024),$c(n)&&qc(n)}function qc(n){n[mi].changeDetectionScheduler?.notify(0);let e=Ir(n);for(;e!==null&&!(e[Fe]&8192||(e[Fe]|=8192,!Mf(e)));)e=Ir(e)}function kv(n,e){if(Ds(n))throw new Te(911,!1);n[Xi]===null&&(n[Xi]=[]),n[Xi].push(e)}function MM(n,e){if(n[Xi]===null)return;let t=n[Xi].indexOf(e);t!==-1&&n[Xi].splice(t,1)}function Ir(n){let e=n[Kt];return xi(e)?e[Kt]:e}function Ef(n){return n[Mc]??=[]}function Sf(n){return n.cleanup??=[]}function EM(n,e,t,i){let r=Ef(e);r.push(t),n.firstCreatePass&&Sf(n).push(i,r.length-1)}var He={lFrame:$v(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Rd=!1;function SM(){return He.lFrame.elementDepthCount}function wM(){He.lFrame.elementDepthCount++}function CM(){He.lFrame.elementDepthCount--}function Uv(){return He.bindingsEnabled}function TM(){return He.skipHydrationRootTNode!==null}function DM(n){return He.skipHydrationRootTNode===n}function AM(){He.skipHydrationRootTNode=null}function ft(){return He.lFrame.lView}function on(){return He.lFrame.tView}function At(n){return He.lFrame.contextLView=n,n[mn]}function It(n){return He.lFrame.contextLView=null,n}function Pn(){let n=Vv();for(;n!==null&&n.type===64;)n=n.parent;return n}function Vv(){return He.lFrame.currentTNode}function IM(){let n=He.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Ho(n,e){let t=He.lFrame;t.currentTNode=n,t.isParent=e}function Bv(){return He.lFrame.isParent}function RM(){He.lFrame.isParent=!1}function NM(){return He.lFrame.contextLView}function Hv(){return Rd}function Ig(n){let e=Rd;return Rd=n,e}function PM(){let n=He.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function OM(){return He.lFrame.bindingIndex}function FM(n){return He.lFrame.bindingIndex=n}function wf(){return He.lFrame.bindingIndex++}function zv(n){let e=He.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function LM(){return He.lFrame.inI18n}function kM(n,e){let t=He.lFrame;t.bindingIndex=t.bindingRootIndex=n,Nd(e)}function UM(){return He.lFrame.currentDirectiveIndex}function Nd(n){He.lFrame.currentDirectiveIndex=n}function VM(n){let e=He.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Gv(){return He.lFrame.currentQueryIndex}function Cf(n){He.lFrame.currentQueryIndex=n}function BM(n){let e=n[Ge];return e.type===2?e.declTNode:e.type===1?n[jn]:null}function Wv(n,e,t){if(t&We.SkipSelf){let r=e,s=n;for(;r=r.parent,r===null&&!(t&We.Host);)if(r=BM(s),r===null||(s=s[Cs],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=He.lFrame=jv();return i.currentTNode=e,i.lView=n,!0}function Tf(n){let e=jv(),t=n[Ge];He.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function jv(){let n=He.lFrame,e=n===null?null:n.child;return e===null?$v(n):e}function $v(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function qv(){let n=He.lFrame;return He.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Xv=qv;function Df(){let n=qv();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function HM(n){return(He.lFrame.contextLView=bM(n,He.lFrame.contextLView))[mn]}function Ur(){return He.lFrame.selectedIndex}function Rr(n){He.lFrame.selectedIndex=n}function Yv(){let n=He.lFrame;return Pv(n.tView,n.selectedIndex)}function Zv(){return He.lFrame.currentNamespace}var Jv=!0;function Af(){return Jv}function If(n){Jv=n}function zM(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Av(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function Kv(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function uc(n,e,t){Qv(n,e,3,t)}function dc(n,e,t,i){(n[Fe]&3)===t&&Qv(n,e,t,i)}function hd(n,e){let t=n[Fe];(t&3)===e&&(t&=16383,t+=1,n[Fe]=t)}function Qv(n,e,t,i){let r=i!==void 0?n[vs]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[vs]+=65536),(a<s||s==-1)&&(GM(n,t,e,c),n[vs]=(n[vs]&4294901760)+c+2),c++}function Rg(n,e){yt(4,n,e);let t=Ze(null);try{e.call(n)}finally{Ze(t),yt(5,n,e)}}function GM(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Fe]>>14<n[vs]>>16&&(n[Fe]&3)===e&&(n[Fe]+=16384,Rg(a,s)):Rg(a,s)}var _s=-1,Nr=class{factory;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i){this.factory=e,this.canSeeViewProviders=t,this.injectImpl=i}};function WM(n){return(n.flags&8)!==0}function jM(n){return(n.flags&16)!==0}function $M(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];XM(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function qM(n){return n===3||n===4||n===6}function XM(n){return n.charCodeAt(0)===64}function Po(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?Ng(n,t,r,null,e[++i]):Ng(n,t,r,null,null))}}return n}function Ng(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function ey(n){return n!==_s}function Tc(n){return n&32767}function YM(n){return n>>16}function Dc(n,e){let t=YM(n),i=e;for(;t>0;)i=i[Cs],t--;return i}var Pd=!0;function Ac(n){let e=Pd;return Pd=n,e}var ZM=256,ty=ZM-1,ny=5,JM=0,Qn={};function KM(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Do)&&(i=t[Do]),i==null&&(i=t[Do]=JM++);let r=i&ty,s=1<<r;e.data[n+(r>>ny)]|=s}function Ic(n,e){let t=iy(n,e);if(t!==-1)return t;let i=e[Ge];i.firstCreatePass&&(n.injectorIndex=e.length,pd(i.data,n),pd(e,null),pd(i.blueprint,null));let r=Rf(n,e),s=n.injectorIndex;if(ey(r)){let o=Tc(r),a=Dc(r,e),c=a[Ge].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function pd(n,e){n.push(0,0,0,0,0,0,0,0,e)}function iy(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Rf(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=cy(r),i===null)return _s;if(t++,r=r[Cs],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return _s}function Od(n,e,t){KM(n,e,t)}function ry(n,e,t){if(t&We.Optional||n!==void 0)return n;gf(e,"NodeInjector")}function sy(n,e,t,i){if(t&We.Optional&&i===void 0&&(i=null),(t&(We.Self|We.Host))===0){let r=n[bs],s=pn(void 0);try{return r?r.get(e,i,t&We.Optional):mv(e,i,t&We.Optional)}finally{pn(s)}}return ry(i,e,t)}function oy(n,e,t,i=We.Default,r){if(n!==null){if(e[Fe]&2048&&!(i&We.Self)){let o=nE(n,e,t,i,Qn);if(o!==Qn)return o}let s=ay(n,e,t,i,Qn);if(s!==Qn)return s}return sy(e,t,i,r)}function ay(n,e,t,i,r){let s=eE(t);if(typeof s=="function"){if(!Wv(e,n,i))return i&We.Host?ry(r,t,i):sy(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&We.Optional))gf(t);else return o}finally{Xv()}}else if(typeof s=="number"){let o=null,a=iy(n,e),c=_s,l=i&We.Host?e[ti][jn]:null;for((a===-1||i&We.SkipSelf)&&(c=a===-1?Rf(n,e):e[a+8],c===_s||!Og(i,!1)?a=-1:(o=e[Ge],a=Tc(c),e=Dc(c,e)));a!==-1;){let u=e[Ge];if(Pg(s,a,u.data)){let d=QM(a,e,t,o,i,l);if(d!==Qn)return d}c=e[a+8],c!==_s&&Og(i,e[Ge].data[a+8]===l)&&Pg(s,a,e)?(o=u,a=Tc(c),e=Dc(c,e)):a=-1}}return r}function QM(n,e,t,i,r,s){let o=e[Ge],a=o.data[n+8],c=i==null?Ts(a)&&Pd:i!=o&&(a.type&3)!==0,l=r&We.Host&&s===a,u=fc(a,o,t,c,l);return u!==null?Oo(e,o,u,a,r):Qn}function fc(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,h=r?a+u:l;for(let f=d;f<h;f++){let g=o[f];if(f<c&&t===g||f>=c&&g.type===t)return f}if(r){let f=o[c];if(f&&ni(f)&&f.type===t)return c}return null}function Oo(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof Nr){let a=s;a.resolving&&hv(kb(o[t]));let c=Ac(a.canSeeViewProviders);a.resolving=!0;let l,u=a.injectImpl?pn(a.injectImpl):null,d=Wv(n,i,We.Default);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&zM(t,o[t],e)}finally{u!==null&&pn(u),Ac(c),a.resolving=!1,Xv()}}return s}function eE(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Do)?n[Do]:void 0;return typeof e=="number"?e>=0?e&ty:tE:e}function Pg(n,e,t){let i=1<<n;return!!(t[e+(n>>ny)]&i)}function Og(n,e){return!(n&We.Self)&&!(n&We.Host&&e)}var wr=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return oy(this._tNode,this._lView,e,zc(i),t)}};function tE(){return new wr(Pn(),ft())}function Nf(n){return Vo(()=>{let e=n.prototype.constructor,t=e[mc]||Fd(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[mc]||Fd(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function Fd(n){return lv(n)?()=>{let e=Fd(sn(n));return e&&e()}:Cr(n)}function nE(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Fe]&2048&&!wc(o);){let a=ay(s,o,t,i|We.Self,Qn);if(a!==Qn)return a;let c=s.parent;if(!c){let l=o[wv];if(l){let u=l.get(t,Qn,i);if(u!==Qn)return u}c=cy(o),o=o[Cs]}s=c}return r}function cy(n){let e=n[Ge],t=e.type;return t===2?e.declTNode:t===1?n[jn]:null}function Fg(n,e=null,t=null,i){let r=iE(n,e,t,i);return r.resolveInjectorInitializers(),r}function iE(n,e=null,t=null,i,r=new Set){let s=[t||An,tM(n)];return i=i||(typeof n=="object"?void 0:In(n)),new Ro(s,e||_f(),i||null,r)}var Es=class n{static THROW_IF_NOT_FOUND=Sr;static NULL=new xc;static create(e,t){if(Array.isArray(e))return Fg({name:""},t,e,"");{let i=e.name??"";return Fg({name:i},e.parent,e.providers,i)}}static \u0275prov=tt({token:n,providedIn:"any",factory:()=>Xe(yv)});static __NG_ELEMENT_ID__=-1};var rE=new Ne("");rE.__NG_ELEMENT_ID__=n=>{let e=Pn();if(e===null)throw new Te(204,!1);if(e.type&2)return e.value;if(n&We.Optional)return null;throw new Te(204,!1)};var ly=!1,Pf=(()=>{class n{static __NG_ELEMENT_ID__=sE;static __NG_ENV_ID__=t=>t}return n})(),Ld=class extends Pf{_lView;constructor(e){super(),this._lView=e}onDestroy(e){let t=this._lView;return Ds(t)?(e(),()=>{}):(kv(t,e),()=>MM(t,e))}};function sE(){return new Ld(ft())}var Fo=class{},uy=new Ne("",{providedIn:"root",factory:()=>!1});var dy=new Ne(""),fy=new Ne(""),As=(()=>{class n{taskId=0;pendingTasks=new Set;get _hasPendingTasks(){return this.hasPendingTasks.value}hasPendingTasks=new pi(!1);add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static \u0275prov=tt({token:n,providedIn:"root",factory:()=>new n})}return n})();var kd=class extends dn{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,dM()&&(this.destroyRef=qe(Pf,{optional:!0})??void 0,this.pendingTasks=qe(As,{optional:!0})??void 0)}emit(e){let t=Ze(null);try{super.next(e)}finally{Ze(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof rn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Ot=kd;function Rc(...n){}function hy(n){let e,t;function i(){n=Rc;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Lg(n){return queueMicrotask(()=>n()),()=>{n=Rc}}var Of="isAngularZone",Nc=Of+"_ID",oE=0,Jt=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Ot(!1);onMicrotaskEmpty=new Ot(!1);onStable=new Ot(!1);onError=new Ot(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=ly}=e;if(typeof Zone>"u")throw new Te(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,lE(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Of)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Te(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Te(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,aE,Rc,Rc);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},aE={};function Ff(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function cE(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){hy(()=>{n.callbackScheduled=!1,Ud(n),n.isCheckStableRunning=!0,Ff(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Ud(n)}function lE(n){let e=()=>{cE(n)},t=oE++;n._inner=n._inner.fork({name:"angular",properties:{[Of]:!0,[Nc]:t,[Nc+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(uE(c))return i.invokeTask(s,o,a,c);try{return kg(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Ug(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return kg(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!dE(c)&&e(),Ug(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Ud(n),Ff(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Ud(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function kg(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Ug(n){n._nesting--,Ff(n)}var Vd=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Ot;onMicrotaskEmpty=new Ot;onStable=new Ot;onError=new Ot;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function uE(n){return py(n,"__ignore_ng_zone__")}function dE(n){return py(n,"__scheduler_tick__")}function py(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var vi=class{_console=console;handleError(e){this._console.error("ERROR",e)}},fE=new Ne("",{providedIn:"root",factory:()=>{let n=qe(Jt),e=qe(vi);return t=>n.runOutsideAngular(()=>e.handleError(t))}});function hE(){return Is(Pn(),ft())}function Is(n,e){return new Mi(bi(n,e))}var Mi=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=hE}return n})();function pE(n){return n instanceof Mi?n.nativeElement:n}function mE(n){return typeof n=="function"&&n[Hn]!==void 0}function zo(n,e){let t=$u(n,e?.equal),i=t[Hn];return t.set=r=>Va(i,r),t.update=r=>qu(i,r),t.asReadonly=gE.bind(t),t}function gE(){let n=this[Hn];if(n.readonlyFn===void 0){let e=()=>this();e[Hn]=n,n.readonlyFn=e}return n.readonlyFn}function my(n){return mE(n)&&typeof n.set=="function"}function vE(){return this._results[Symbol.iterator]()}var Bd=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new dn}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=Yb(e);(this._changesDetected=!Xb(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=vE};function gy(n){return(n.flags&128)===128}var vy=(function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n})(vy||{}),yy=new Map,yE=0;function _E(){return yE++}function xE(n){yy.set(n[jc],n)}function Hd(n){yy.delete(n[jc])}var Vg="__ngContext__";function Go(n,e){Yi(e)?(n[Vg]=e[jc],xE(e)):n[Vg]=e}function _y(n){return by(n[No])}function xy(n){return by(n[Wn])}function by(n){for(;n!==null&&!xi(n);)n=n[Wn];return n}var zd;function My(n){zd=n}function bE(){if(zd!==void 0)return zd;if(typeof document<"u")return document;throw new Te(210,!1)}var Lf=new Ne("",{providedIn:"root",factory:()=>ME}),ME="ng",kf=new Ne(""),Wo=new Ne("",{providedIn:"platform",factory:()=>"unknown"});var Uf=new Ne("",{providedIn:"root",factory:()=>bE().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var EE="h",SE="b";var Ey=!1,wE=new Ne("",{providedIn:"root",factory:()=>Ey});var Sy=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Sy||{}),Xc=new Ne(""),Bg=new Set;function CE(n){Bg.has(n)||(Bg.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var TE=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=tt({token:n,providedIn:"root",factory:()=>new n})}return n})();var DE=(n,e,t,i)=>{};function AE(n,e,t,i){DE(n,e,t,i)}var IE=()=>null;function wy(n,e,t=!1){return IE(n,e,t)}function Cy(n,e){let t=n.contentQueries;if(t!==null){let i=Ze(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Cf(s),a.contentQueries(2,e[o],o)}}}finally{Ze(i)}}}function Gd(n,e,t){Cf(0);let i=Ze(null);try{e(n,t)}finally{Ze(i)}}function Ty(n,e,t){if(Tv(e)){let i=Ze(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{Ze(i)}}}var si=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n})(si||{});var Pc=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${av})`}};function Dy(n){return n instanceof Pc?n.changingThisBreaksApplicationSecurity:n}function RE(n,e){let t=NE(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${av})`)}return t===e}function NE(n){return n instanceof Pc&&n.getTypeName()||null}var PE=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function OE(n){return n=String(n),n.match(PE)?n:"unsafe:"+n}function FE(n,e){return n.createText(e)}function LE(n,e,t){n.setValue(e,t)}function Ay(n,e,t){return n.createElement(e,t)}function Oc(n,e,t,i,r){n.insertBefore(e,t,i,r)}function Iy(n,e,t){n.appendChild(e,t)}function Hg(n,e,t,i,r){i!==null?Oc(n,e,t,i,r):Iy(n,e,t)}function kE(n,e,t){n.removeChild(null,e,t)}function UE(n,e,t){n.setAttribute(e,"style",t)}function VE(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Ry(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&$M(n,e,i),r!==null&&VE(n,e,r),s!==null&&UE(n,e,s)}var Ny=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n[n.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",n})(Ny||{});function Py(n){let e=BE();return e?e.sanitize(Ny.URL,n)||"":RE(n,"URL")?Dy(n):OE(Ao(n))}function BE(){let n=ft();return n&&n[mi].sanitizer}function jo(n){return n.ownerDocument.defaultView}function HE(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var Oy="ng-template";function zE(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&HE(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Vf(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Vf(n){return n.type===4&&n.value!==Oy}function GE(n,e,t){let i=n.type===4&&!t?Oy:n.value;return e===i}function WE(n,e,t){let i=4,r=n.attrs,s=r!==null?qE(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!Gn(i)&&!Gn(c))return!1;if(o&&Gn(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!GE(n,c,t)||c===""&&e.length===1){if(Gn(i))return!1;o=!0}}else if(i&8){if(r===null||!zE(n,r,c,t)){if(Gn(i))return!1;o=!0}}else{let l=e[++a],u=jE(c,r,Vf(n),t);if(u===-1){if(Gn(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(Gn(i))return!1;o=!0}}}}return Gn(i)||o}function Gn(n){return(n&1)===0}function jE(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return XE(e,n)}function $E(n,e,t=!1){for(let i=0;i<e.length;i++)if(WE(n,e[i],t))return!0;return!1}function qE(n){for(let e=0;e<n.length;e++){let t=n[e];if(qM(t))return e}return n.length}function XE(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function zg(n,e){return n?":not("+e.trim()+")":e}function YE(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!Gn(o)&&(e+=zg(s,r),r=""),i=o,s=s||!Gn(i);t++}return r!==""&&(e+=zg(s,r)),e}function ZE(n){return n.map(YE).join(",")}function JE(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!Gn(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var Ei={};function Bf(n,e,t,i,r,s,o,a,c,l,u){let d=Nn+i,h=d+r,f=KE(d,h),g=typeof l=="function"?l():l;return f[Ge]={type:n,blueprint:f,template:t,queries:null,viewQuery:a,declTNode:e,data:f.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function KE(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:Ei);return t}function QE(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Bf(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Hf(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[_i]=r,d[Fe]=i|4|128|8|64|1024,(l!==null||n&&n[Fe]&2048)&&(d[Fe]|=2048),Fv(d),d[Kt]=d[Cs]=n,d[mn]=t,d[mi]=o||n&&n[mi],d[Gt]=a||n&&n[Gt],d[bs]=c||n&&n[bs]||null,d[jn]=s,d[jc]=_E(),d[bc]=u,d[wv]=l,d[ti]=e.type==2?n[ti]:d,d}function eS(n,e,t){let i=bi(e,n),r=QE(t),s=n[mi].rendererFactory,o=zf(n,Hf(n,r,null,Fy(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function Fy(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function Ly(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function zf(n,e){return n[No]?n[Dg][Wn]=e:n[No]=e,n[Dg]=e,e}function ne(n=1){ky(on(),ft(),Ur()+n,!1)}function ky(n,e,t,i){if(!i)if((e[Fe]&3)===3){let s=n.preOrderCheckHooks;s!==null&&uc(e,s,t)}else{let s=n.preOrderHooks;s!==null&&dc(e,s,0,t)}Rr(t)}var Yc=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(Yc||{});function Wd(n,e,t,i){let r=Ze(null);try{let[s,o,a]=n.inputs[t],c=null;(o&Yc.SignalBased)!==0&&(c=e[s][Hn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):Dv(e,c,s,i)}finally{Ze(r)}}function Uy(n,e,t,i,r){let s=Ur(),o=i&2;try{Rr(-1),o&&e.length>Nn&&ky(n,e,Nn,!1),yt(o?2:0,r),t(i,r)}finally{Rr(s),yt(o?3:1,r)}}function Gf(n,e,t){oS(n,e,t),(t.flags&64)===64&&aS(n,e,t)}function Vy(n,e,t=bi){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function tS(n,e,t,i){let s=i.get(wE,Ey)||t===si.ShadowDom,o=n.selectRootElement(e,s);if(o?.tagName?.toLowerCase()==="script")throw new Te(905,!1);return nS(o),o}function nS(n){iS(n)}var iS=()=>null;function rS(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function By(n,e,t,i,r,s,o,a){if(!a&&Wf(e,n,t,i,r)){Ts(e)&&sS(t,e.index);return}if(e.type&3){let c=bi(e,t);i=rS(i),r=o!=null?o(r,e.value||"",i):r,s.setProperty(c,i,r)}else e.type&12}function sS(n,e){let t=ri(e,n);t[Fe]&16||(t[Fe]|=64)}function oS(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Ts(t)&&eS(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Ic(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=Oo(e,n,o,t);if(Go(c,e),s!==null&&lS(e,o-i,c,a,t,s),ni(a)){let l=ri(t.index,e);l[mn]=Oo(e,n,o,t)}}}function aS(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=UM();try{Rr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Nd(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&cS(c,l)}}finally{Rr(-1),Nd(o)}}function cS(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function Hy(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];$E(e,s.selectors,!1)&&(i??=[],ni(s)?i.unshift(s):i.push(s))}return i}function lS(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];Wd(i,t,c,l)}}function uS(n,e){let t=n[bs],i=t?t.get(vi,null):null;i&&i.handleError(e)}function Wf(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],d=e.data[l];Wd(d,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];Wd(u,l,i,r),a=!0}return a}function dS(n,e){let t=ri(e,n),i=t[Ge];fS(i,t);let r=t[_i];r!==null&&t[bc]===null&&(t[bc]=wy(r,t[bs])),yt(18),jf(i,t,t[mn]),yt(19,t[mn])}function fS(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function jf(n,e,t){Tf(e);try{let i=n.viewQuery;i!==null&&Gd(1,i,t);let r=n.template;r!==null&&Uy(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[gi]?.finishViewCreation(n),n.staticContentQueries&&Cy(n,e),n.staticViewQueries&&Gd(2,n.viewQuery,t);let s=n.components;s!==null&&hS(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Fe]&=-5,Df()}}function hS(n,e){for(let t=0;t<e.length;t++)dS(n,e[t])}function pS(n,e,t,i){let r=Ze(null);try{let s=e.tView,a=n[Fe]&4096?4096:16,c=Hf(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Dr]=l;let u=n[gi];return u!==null&&(c[gi]=u.createEmbeddedView(s)),jf(s,c,t),c}finally{Ze(r)}}function Gg(n,e){return!e||e.firstChild===null||gy(n)}var mS;function $f(n,e){return mS(n,e)}var yi=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(yi||{});function zy(n){return(n.flags&32)===32}function ys(n,e,t,i,r){if(i!=null){let s,o=!1;xi(i)?s=i:Yi(i)&&(o=!0,i=i[_i]);let a=ii(i);n===0&&t!==null?r==null?Iy(e,t,a):Oc(e,t,a,r||null,!0):n===1&&t!==null?Oc(e,t,a,r||null,!0):n===2?kE(e,a,o):n===3&&e.destroyNode(a),s!=null&&TS(e,n,s,t,r)}}function gS(n,e){Gy(n,e),e[_i]=null,e[jn]=null}function vS(n,e,t,i,r,s){i[_i]=r,i[jn]=e,Zc(n,i,t,1,r,s)}function Gy(n,e){e[mi].changeDetectionScheduler?.notify(9),Zc(n,e,e[Gt],2,null,null)}function yS(n){let e=n[No];if(!e)return md(n[Ge],n);for(;e;){let t=null;if(Yi(e))t=e[No];else{let i=e[Mn];i&&(t=i)}if(!t){for(;e&&!e[Wn]&&e!==n;)Yi(e)&&md(e[Ge],e),e=e[Kt];e===null&&(e=n),Yi(e)&&md(e[Ge],e),t=e&&e[Wn]}e=t}}function qf(n,e){let t=n[Ms],i=t.indexOf(e);t.splice(i,1)}function Wy(n,e){if(Ds(e))return;let t=e[Gt];t.destroyNode&&Zc(n,e,t,3,null,null),yS(e)}function md(n,e){if(Ds(e))return;let t=Ze(null);try{e[Fe]&=-129,e[Fe]|=256,e[Rn]&&Gu(e[Rn]),xS(n,e),_S(n,e),e[Ge].type===1&&e[Gt].destroy();let i=e[Dr];if(i!==null&&xi(e[Kt])){i!==e[Kt]&&qf(i,e);let r=e[gi];r!==null&&r.detachView(n)}Hd(e)}finally{Ze(t)}}function _S(n,e){let t=n.cleanup,i=e[Mc];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[Mc]=null);let r=e[Xi];if(r!==null){e[Xi]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[Ec];if(s!==null){e[Ec]=null;for(let o of s)o.destroy()}}function xS(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Nr)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];yt(4,a,c);try{c.call(a)}finally{yt(5,a,c)}}else{yt(4,r,s);try{s.call(r)}finally{yt(5,r,s)}}}}}function bS(n,e,t){return MS(n,e.parent,t)}function MS(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[_i];if(Ts(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===si.None||r===si.Emulated)return null}return bi(i,t)}function ES(n,e,t){return wS(n,e,t)}function SS(n,e,t){return n.type&40?bi(n,t):null}var wS=SS,Wg;function Xf(n,e,t,i){let r=bS(n,i,e),s=e[Gt],o=i.parent||e[jn],a=ES(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)Hg(s,r,t[c],a,!1);else Hg(s,r,t,a,!1);Wg!==void 0&&Wg(s,i,e,t,r)}function To(n,e){if(e!==null){let t=e.type;if(t&3)return bi(e,n);if(t&4)return jd(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return To(n,i);{let r=n[e.index];return xi(r)?jd(-1,r):ii(r)}}else{if(t&128)return To(n,e.next);if(t&32)return $f(e,n)()||ii(n[e.index]);{let i=jy(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Ir(n[ti]);return To(r,i)}else return To(n,e.next)}}}return null}function jy(n,e){if(e!==null){let i=n[ti][jn],r=e.projection;return i.projection[r]}return null}function jd(n,e){let t=Mn+n+1;if(t<e.length){let i=e[t],r=i[Ge].firstChild;if(r!==null)return To(i,r)}return e[Ar]}function Yf(n,e,t,i,r,s,o){for(;t!=null;){if(t.type===128){t=t.next;continue}let a=i[t.index],c=t.type;if(o&&e===0&&(a&&Go(ii(a),i),t.flags|=2),!zy(t))if(c&8)Yf(n,e,t.child,i,r,s,!1),ys(e,n,r,a,s);else if(c&32){let l=$f(t,i),u;for(;u=l();)ys(e,n,r,u,s);ys(e,n,r,a,s)}else c&16?CS(n,e,i,t,r,s):ys(e,n,r,a,s);t=o?t.projectionNext:t.next}}function Zc(n,e,t,i,r,s){Yf(t,i,n.firstChild,e,r,s,!1)}function CS(n,e,t,i,r,s){let o=t[ti],c=o[jn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];ys(e,n,r,u,s)}else{let l=c,u=o[Kt];gy(i)&&(l.flags|=128),Yf(n,e,l,u,r,s,!0)}}function TS(n,e,t,i,r){let s=t[Ar],o=ii(t);s!==o&&ys(e,n,i,s,r);for(let a=Mn;a<t.length;a++){let c=t[a];Zc(c[Ge],c,n,e,i,s)}}function DS(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:yi.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=yi.Important),n.setStyle(t,i,r,s))}}function Fc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(ii(s)),xi(s)&&AS(s,i);let o=t.type;if(o&8)Fc(n,e,t.child,i);else if(o&32){let a=$f(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=jy(e,t);if(Array.isArray(a))i.push(...a);else{let c=Ir(e[ti]);Fc(c[Ge],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function AS(n,e){for(let t=Mn;t<n.length;t++){let i=n[t],r=i[Ge].firstChild;r!==null&&Fc(i[Ge],i,r,e)}n[Ar]!==n[_i]&&e.push(n[Ar])}function $y(n){if(n[fd]!==null){for(let e of n[fd])e.impl.addSequence(e);n[fd].length=0}}var qy=[];function IS(n){return n[Rn]??RS(n)}function RS(n){let e=qy.pop()??Object.create(PS);return e.lView=n,e}function NS(n){n.lView[Rn]!==n&&(n.lView=null,qy.push(n))}var PS=wt(dt({},Mo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{qc(n.lView)},consumerOnSignalRead(){this.lView[Rn]=this}});function OS(n){let e=n[Rn]??Object.create(FS);return e.lView=n,e}var FS=wt(dt({},Mo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Ir(n.lView);for(;e&&!Xy(e[Ge]);)e=Ir(e);e&&Lv(e)},consumerOnSignalRead(){this.lView[Rn]=this}});function Xy(n){return n.type!==2}function Yy(n){if(n[Ec]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Ec])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Fe]&8192)}}var LS=100;function Zy(n,e=!0,t=0){let r=n[mi].rendererFactory,s=!1;s||r.begin?.();try{kS(n,t)}catch(o){throw e&&uS(n,o),o}finally{s||r.end?.()}}function kS(n,e){let t=Hv();try{Ig(!0),$d(n,e);let i=0;for(;$c(n);){if(i===LS)throw new Te(103,!1);i++,$d(n,1)}}finally{Ig(t)}}function US(n,e,t,i){if(Ds(e))return;let r=e[Fe],s=!1,o=!1;Tf(e);let a=!0,c=null,l=null;s||(Xy(n)?(l=IS(e),c=La(l)):ku()===null?(a=!1,l=OS(e),c=La(l)):e[Rn]&&(Gu(e[Rn]),e[Rn]=null));try{Fv(e),FM(n.bindingStartIndex),t!==null&&Uy(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let f=n.preOrderCheckHooks;f!==null&&uc(e,f,null)}else{let f=n.preOrderHooks;f!==null&&dc(e,f,0,null),hd(e,0)}if(o||VS(e),Yy(e),Jy(e,0),n.contentQueries!==null&&Cy(n,e),!s)if(u){let f=n.contentCheckHooks;f!==null&&uc(e,f)}else{let f=n.contentHooks;f!==null&&dc(e,f,1),hd(e,1)}HS(n,e);let d=n.components;d!==null&&Qy(e,d,0);let h=n.viewQuery;if(h!==null&&Gd(2,h,i),!s)if(u){let f=n.viewCheckHooks;f!==null&&uc(e,f)}else{let f=n.viewHooks;f!==null&&dc(e,f,2),hd(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[dd]){for(let f of e[dd])f();e[dd]=null}s||($y(e),e[Fe]&=-73)}catch(u){throw s||qc(e),u}finally{l!==null&&(Hu(l,c),a&&NS(l)),Df()}}function Jy(n,e){for(let t=_y(n);t!==null;t=xy(t))for(let i=Mn;i<t.length;i++){let r=t[i];Ky(r,e)}}function VS(n){for(let e=_y(n);e!==null;e=xy(e)){if(!(e[Fe]&2))continue;let t=e[Ms];for(let i=0;i<t.length;i++){let r=t[i];Lv(r)}}}function BS(n,e,t){yt(18);let i=ri(e,n);Ky(i,t),yt(19,i[mn])}function Ky(n,e){Mf(n)&&$d(n,e)}function $d(n,e){let i=n[Ge],r=n[Fe],s=n[Rn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&zu(s)),o||=!1,s&&(s.dirty=!1),n[Fe]&=-9217,o)US(i,n,i.template,n[mn]);else if(r&8192){Yy(n),Jy(n,1);let a=i.components;a!==null&&Qy(n,a,1),$y(n)}}function Qy(n,e,t){for(let i=0;i<e.length;i++)BS(n,e[i],t)}function HS(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Rr(~r);else{let s=r,o=t[++i],a=t[++i];kM(o,s);let c=e[s];yt(24,c),a(2,c),yt(25,c)}}}finally{Rr(-1)}}function Zf(n,e){let t=Hv()?64:1088;for(n[mi].changeDetectionScheduler?.notify(e);n;){n[Fe]|=t;let i=Ir(n);if(wc(n)&&!i)return n;n=i}return null}function e_(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function zS(n,e,t,i=!0){let r=e[Ge];if(GS(r,e,n,t),i){let o=jd(t,n),a=e[Gt],c=a.parentNode(n[Ar]);c!==null&&vS(r,n[jn],a,e,c,o)}let s=e[bc];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function qd(n,e){if(n.length<=Mn)return;let t=Mn+e,i=n[t];if(i){let r=i[Dr];r!==null&&r!==n&&qf(r,i),e>0&&(n[t-1][Wn]=i[Wn]);let s=yc(n,Mn+e);gS(i[Ge],i);let o=s[gi];o!==null&&o.detachView(s[Ge]),i[Kt]=null,i[Wn]=null,i[Fe]&=-129}return i}function GS(n,e,t,i){let r=Mn+i,s=t.length;i>0&&(t[r-1][Wn]=e),i<s-Mn?(e[Wn]=t[r],vv(t,Mn+i,e)):(t.push(e),e[Wn]=null),e[Kt]=t;let o=e[Dr];o!==null&&t!==o&&t_(o,e);let a=e[gi];a!==null&&a.insertView(n),Id(e),e[Fe]|=128}function t_(n,e){let t=n[Ms],i=e[Kt];if(Yi(i))n[Fe]|=2;else{let r=i[Kt][ti];e[ti]!==r&&(n[Fe]|=2)}t===null?n[Ms]=[e]:t.push(e)}var Lo=class{_lView;_cdRefInjectingView;notifyErrorHandler;_appRef=null;_attachedToViewContainer=!1;get rootNodes(){let e=this._lView,t=e[Ge];return Fc(t,e,t.firstChild,[])}constructor(e,t,i=!0){this._lView=e,this._cdRefInjectingView=t,this.notifyErrorHandler=i}get context(){return this._lView[mn]}set context(e){this._lView[mn]=e}get destroyed(){return Ds(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Kt];if(xi(e)){let t=e[Sc],i=t?t.indexOf(this):-1;i>-1&&(qd(e,i),yc(t,i))}this._attachedToViewContainer=!1}Wy(this._lView[Ge],this._lView)}onDestroy(e){kv(this._lView,e)}markForCheck(){Zf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Fe]&=-129}reattach(){Id(this._lView),this._lView[Fe]|=128}detectChanges(){this._lView[Fe]|=1024,Zy(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Te(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=wc(this._lView),t=this._lView[Dr];t!==null&&!e&&qf(t,this._lView),Gy(this._lView[Ge],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Te(902,!1);this._appRef=e;let t=wc(this._lView),i=this._lView[Dr];i!==null&&!t&&t_(i,this._lView),Id(this._lView)}};var Pr=(()=>{class n{static __NG_ELEMENT_ID__=$S}return n})(),WS=Pr,jS=class extends WS{_declarationLView;_declarationTContainer;elementRef;constructor(e,t,i){super(),this._declarationLView=e,this._declarationTContainer=t,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,t){return this.createEmbeddedViewImpl(e,t)}createEmbeddedViewImpl(e,t,i){let r=pS(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:t,dehydratedView:i});return new Lo(r)}};function $S(){return Jf(Pn(),ft())}function Jf(n,e){return n.type&4?new jS(e,n,Is(n,e)):null}function Kf(n,e,t,i,r){let s=n.data[e];if(s===null)s=qS(n,e,t,i,r),LM()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=IM();s.injectorIndex=o===null?-1:o.injectorIndex}return Ho(s,!0),s}function qS(n,e,t,i,r){let s=Vv(),o=Bv(),a=o?s:s&&s.parent,c=n.data[e]=YS(n,a,t,e,i,r);return XS(n,c,s,o),c}function XS(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function YS(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return TM()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Zv(),attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var S2=new RegExp(`^(\\d+)*(${SE}|${EE})*(.*)`);var ZS=()=>null;function jg(n,e){return ZS(n,e)}var JS=class{},n_=class{},Xd=class{resolveComponentFactory(e){throw Error(`No component factory found for ${In(e)}.`)}},Qf=class{static NULL=new Xd},Ss=class{},$o=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>KS()}return n})();function KS(){let n=ft(),e=Pn(),t=ri(e.index,n);return(Yi(t)?t:n)[Gt]}var QS=(()=>{class n{static \u0275prov=tt({token:n,providedIn:"root",factory:()=>null})}return n})();var gd={},Yd=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){i=zc(i);let r=this.injector.get(e,gd,i);return r!==gd||t===gd?r:this.parentInjector.get(e,t,i)}};function $g(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=bg(r,a);else if(s==2){let c=a,l=e[++o];i=bg(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Se(n,e=We.Default){let t=ft();if(t===null)return Xe(n,e);let i=Pn();return oy(i,t,sn(n),e)}function i_(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a,c=null,l=null,u=tw(o);u===null?a=o:[a,c,l]=u,rw(n,e,t,a,s,c,l)}s!==null&&i!==null&&ew(t,i,s)}function ew(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Te(-301,!1);i.push(e[r],s)}}function tw(n){let e=null,t=!1;for(let o=0;o<n.length;o++){let a=n[o];if(o===0&&ni(a)&&(e=a),a.findHostDirectiveDefs!==null){t=!0;break}}if(!t)return null;let i=null,r=null,s=null;for(let o of n)o.findHostDirectiveDefs!==null&&(i??=[],r??=new Map,s??=new Map,nw(o,i,s,r)),o===e&&(i??=[],i.push(o));return i!==null?(i.push(...e===null?n:n.slice(1)),[i,r,s]):null}function nw(n,e,t,i){let r=e.length;n.findHostDirectiveDefs(n,e,i),t.set(n,[r,e.length-1])}function iw(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function rw(n,e,t,i,r,s,o){let a=i.length,c=!1;for(let h=0;h<a;h++){let f=i[h];!c&&ni(f)&&(c=!0,iw(n,t,h)),Od(Ic(t,e),n,f.type)}uw(t,n.data.length,a);for(let h=0;h<a;h++){let f=i[h];f.providersResolver&&f.providersResolver(f)}let l=!1,u=!1,d=Ly(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let h=0;h<a;h++){let f=i[h];if(t.mergedAttrs=Po(t.mergedAttrs,f.hostAttrs),ow(n,t,e,d,f),lw(d,f,r),o!==null&&o.has(f)){let[v,m]=o.get(f);t.directiveToIndex.set(f.type,[d,v+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(f))&&t.directiveToIndex.set(f.type,d);f.contentQueries!==null&&(t.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(t.flags|=64);let g=f.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}sw(n,t,s)}function sw(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))qg(0,e,r,i),qg(1,e,r,i),Yg(e,i,!1);else{let s=t.get(r);Xg(0,e,s,i),Xg(1,e,s,i),Yg(e,i,!0)}}}function qg(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),r_(e,s)}}function Xg(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),r_(e,o)}}function r_(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function Yg(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||Vf(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function ow(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Cr(r.type,!0)),o=new Nr(s,ni(r),Se);n.blueprint[i]=o,t[i]=o,aw(n,e,i,Ly(n,t,r.hostVars,Ei),r)}function aw(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;cw(o)!=a&&o.push(a),o.push(t,i,s)}}function cw(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function lw(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;ni(e)&&(t[""]=n)}}function uw(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function s_(n,e,t,i,r,s,o,a){let c=e.consts,l=Cc(c,o),u=Kf(e,n,2,i,l);return s&&i_(e,t,u,Cc(c,a),r),u.mergedAttrs=Po(u.mergedAttrs,u.attrs),u.attrs!==null&&$g(u,u.attrs,!1),u.mergedAttrs!==null&&$g(u,u.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,u),u}function o_(n,e){Kv(n,e),Tv(e)&&n.queries.elementEnd(e)}var Zd=class extends Qf{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Io(e);return new Lc(t,this.ngModule)}};function dw(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&Yc.SignalBased)!==0};return r&&(s.transform=r),s})}function fw(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function hw(n,e,t){let i=e instanceof ei?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Yd(t,i):t}function pw(n){let e=n.get(Ss,null);if(e===null)throw new Te(407,!1);let t=n.get(QS,null),i=n.get(Fo,null);return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i}}function mw(n,e){let t=(n.selectors[0][0]||"div").toLowerCase();return Ay(e,t,t==="svg"?gM:t==="math"?vM:null)}var Lc=class extends n_{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=dw(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=fw(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=ZE(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r){yt(22);let s=Ze(null);try{let o=this.componentDef,a=i?["ng-version","19.2.25"]:JE(this.componentDef.selectors[0]),c=Bf(0,null,null,1,0,null,null,null,null,[a],null),l=hw(o,r||this.ngModule,e),u=pw(l),d=u.rendererFactory.createRenderer(null,o),h=i?tS(d,i,o.encapsulation,l):mw(o,d),f=Hf(null,c,null,512|Fy(o),null,null,u,d,l,null,wy(h,l,!0));f[Nn]=h,Tf(f);let g=null;try{let v=s_(Nn,c,f,"#host",()=>[this.componentDef],!0,0);h&&(Ry(d,h,v),Go(h,f)),Gf(c,f,v),Ty(c,v,f),o_(c,v),t!==void 0&&gw(v,this.ngContentSelectors,t),g=ri(v.index,f),f[mn]=g[mn],jf(c,f,null)}catch(v){throw g!==null&&Hd(g),Hd(f),v}finally{yt(23),Df()}return new Jd(this.componentType,f)}finally{Ze(s)}}},Jd=class extends JS{_rootLView;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t){super(),this._rootLView=t,this._tNode=Pv(t[Ge],Nn),this.location=Is(this._tNode,t),this.instance=ri(this._tNode.index,t)[mn],this.hostView=this.changeDetectorRef=new Lo(t,void 0,!1),this.componentType=e}setInput(e,t){let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Wf(i,r[Ge],r,e,t);this.previousInputValues.set(e,t);let o=ri(i.index,r);Zf(o,1)}get injector(){return new wr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function gw(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Rs=(()=>{class n{static __NG_ELEMENT_ID__=vw}return n})();function vw(){let n=Pn();return c_(n,ft())}var yw=Rs,a_=class extends yw{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return Is(this._hostTNode,this._hostLView)}get injector(){return new wr(this._hostTNode,this._hostLView)}get parentInjector(){let e=Rf(this._hostTNode,this._hostLView);if(ey(e)){let t=Dc(e,this._hostLView),i=Tc(e),r=t[Ge].data[i+8];return new wr(r,t)}else return new wr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Zg(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Mn}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=jg(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,Gg(this._hostTNode,o)),a}createComponent(e,t,i,r,s){let o=e&&!fM(e),a;if(o)a=t;else{let g=t||{};a=g.index,i=g.injector,r=g.projectableNodes,s=g.environmentInjector||g.ngModuleRef}let c=o?e:new Lc(Io(e)),l=i||this.parentInjector;if(!s&&c.ngModule==null){let v=(o?l:this.parentInjector).get(ei,null);v&&(s=v)}let u=Io(c.componentType??{}),d=jg(this._lContainer,u?.id??null),h=d?.firstChild??null,f=c.create(l,r,h,s);return this.insertImpl(f.hostView,a,Gg(this._hostTNode,d)),f}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(xM(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[Kt],l=new a_(c,c[jn],c[Kt]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return zS(o,r,s,i),e.attachToViewContainerRef(),vv(vd(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Zg(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=qd(this._lContainer,t);i&&(yc(vd(this._lContainer),t),Wy(i[Ge],i))}detach(e){let t=this._adjustIndex(e,-1),i=qd(this._lContainer,t);return i&&yc(vd(this._lContainer),t)!=null?new Lo(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Zg(n){return n[Sc]}function vd(n){return n[Sc]||(n[Sc]=[])}function c_(n,e){let t,i=e[n.index];return xi(i)?t=i:(t=e_(i,e,null,n),e[n.index]=t,zf(e,t)),xw(t,e,n,i),new a_(t,n,e)}function _w(n,e){let t=n[Gt],i=t.createComment(""),r=bi(e,n),s=t.parentNode(r);return Oc(t,s,i,t.nextSibling(r),!1),i}var xw=Ew,bw=()=>!1;function Mw(n,e,t){return bw(n,e,t)}function Ew(n,e,t,i){if(n[Ar])return;let r;t.type&8?r=ii(i):r=_w(e,t),n[Ar]=r}var Kd=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Qd=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)eh(e,t).matches!==null&&this.queries[t].setDirty()}},ef=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=Rw(e):this.predicate=e}},tf=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},nf=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,Sw(t,s)),this.matchTNodeWithReadOption(e,t,fc(t,e,s,!1,!1))}else i===Pr?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,fc(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===Mi||r===Rs||r===Pr&&t.type&4)this.addMatch(t.index,-2);else{let s=fc(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function Sw(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function ww(n,e){return n.type&11?Is(n,e):n.type&4?Jf(n,e):null}function Cw(n,e,t,i){return t===-1?ww(e,n):t===-2?Tw(n,e,i):Oo(n,n[Ge],t,e)}function Tw(n,e,t){if(t===Mi)return Is(e,n);if(t===Pr)return Jf(e,n);if(t===Rs)return c_(e,n)}function l_(n,e,t,i){let r=e[gi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(Cw(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function rf(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=l_(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let d=Mn;d<u.length;d++){let h=u[d];h[Dr]===h[Kt]&&rf(h[Ge],h,l,i)}if(u[Ms]!==null){let d=u[Ms];for(let h=0;h<d.length;h++){let f=d[h];rf(f[Ge],f,l,i)}}}}}return i}function Dw(n,e){return n[gi].queries[e].queryList}function Aw(n,e,t){let i=new Bd((t&4)===4);return EM(n,e,i,i.destroy),(e[gi]??=new Qd).queries.push(new Kd(i))-1}function Iw(n,e,t){let i=on();return i.firstCreatePass&&(Nw(i,new ef(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),Aw(i,ft(),e)}function Rw(n){return n.split(",").map(e=>e.trim())}function Nw(n,e,t){n.queries===null&&(n.queries=new tf),n.queries.track(new nf(e,t))}function eh(n,e){return n.queries.getByIndex(e)}function Pw(n,e){let t=n[Ge],i=eh(t,e);return i.crossesNgTemplate?rf(t,n,e,[]):l_(t,n,i,e)}var kc=class{};var Uc=class extends kc{injector;componentFactoryResolver=new Zd(this);instance=null;constructor(e){super();let t=new Ro([...e.providers,{provide:kc,useValue:this},{provide:Qf,useValue:this.componentFactoryResolver}],e.parent||_f(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function Ow(n,e,t=null){return new Uc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var Fw=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=bv(!1,t.type),r=i.length>0?Ow([i],this._injector,`Standalone[${t.type.name}]`):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=tt({token:n,providedIn:"environment",factory:()=>new n(Xe(ei))})}return n})();function Lt(n){return Vo(()=>{let e=d_(n),t=wt(dt({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===vy.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(Fw).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||si.Emulated,styles:n.styles||An,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&CE("NgStandalone"),f_(t);let i=n.dependencies;return t.directiveDefs=Jg(i,!1),t.pipeDefs=Jg(i,!0),t.id=Bw(t),t})}function Lw(n){return Io(n)||Qb(n)}function kw(n){return n!==null}function Vr(n){return Vo(()=>({type:n.type,bootstrap:n.bootstrap||An,declarations:n.declarations||An,imports:n.imports||An,exports:n.exports||An,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function Uw(n,e){if(n==null)return Tr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=Yc.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function Vw(n){if(n==null)return Tr;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function gn(n){return Vo(()=>{let e=d_(n);return f_(e),e})}function u_(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function d_(n){let e={};return{type:n.type,providersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||Tr,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||An,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:Uw(n.inputs,e),outputs:Vw(n.outputs),debugInfo:null}}function f_(n){n.features?.forEach(e=>e(n))}function Jg(n,e){if(!n)return null;let t=e?eM:Lw;return()=>(typeof n=="function"?n():n).map(i=>t(i)).filter(kw)}function Bw(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function Hw(n){return Object.getPrototypeOf(n.prototype).constructor}function Si(n){let e=Hw(n.type),t=!0,i=[n];for(;e;){let r;if(ni(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new Te(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=yd(n.inputs),o.declaredInputs=yd(n.declaredInputs),o.outputs=yd(n.outputs);let a=r.hostBindings;a&&$w(n,a);let c=r.viewQuery,l=r.contentQueries;if(c&&Ww(n,c),l&&jw(n,l),zw(n,r),Ib(n.outputs,r.outputs),ni(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===Si&&(t=!1)}}e=Object.getPrototypeOf(e)}Gw(i)}function zw(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t])}}function Gw(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=Po(r.hostAttrs,t=Po(t,r.hostAttrs))}}function yd(n){return n===Tr?{}:n===An?[]:n}function Ww(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function jw(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function $w(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}function h_(n){return Xw(n)?Array.isArray(n)||!(n instanceof Map)&&Symbol.iterator in n:!1}function qw(n,e){if(Array.isArray(n))for(let t=0;t<n.length;t++)e(n[t]);else{let t=n[Symbol.iterator](),i;for(;!(i=t.next()).done;)e(i.value)}}function Xw(n){return n!==null&&(typeof n=="function"||typeof n=="object")}function Yw(n,e,t){return n[e]=t}function Or(n,e,t){let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function Zw(n,e,t,i){let r=Or(n,e,t);return Or(n,e+1,i)||r}function Jw(n,e,t,i,r,s,o,a,c){let l=e.consts,u=Kf(e,n,4,o||null,a||null);Uv()&&i_(e,t,u,Cc(l,c),Hy),u.mergedAttrs=Po(u.mergedAttrs,u.attrs),Kv(e,u);let d=u.tView=Bf(2,u,i,r,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,l,null);return e.queries!==null&&(e.queries.template(e,u),d.queries=e.queries.embeddedTView(u)),u}function Kw(n,e,t,i,r,s,o,a,c,l){let u=t+Nn,d=e.firstCreatePass?Jw(u,e,n,i,r,s,o,a,c):e.data[u];Ho(d,!1);let h=Qw(e,n,d,t);Af()&&Xf(e,n,h,d),Go(h,n);let f=e_(h,n,h,d);return n[u]=f,zf(n,f),Mw(f,d,n),xf(d)&&Gf(e,n,d),c!=null&&Vy(n,d,l),d}function kt(n,e,t,i,r,s,o,a){let c=ft(),l=on(),u=Cc(l.consts,s);return Kw(c,l,n,e,t,i,r,u,o,a),kt}var Qw=eC;function eC(n,e,t,i){return If(!0),e[Gt].createComment("")}var p_=new Ne("");var tC=(()=>{class n{static \u0275prov=tt({token:n,providedIn:"root",factory:()=>new sf})}return n})(),sf=class{queuedEffectCount=0;queues=new Map;schedule(e){this.enqueue(e)}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),this.queuedEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||(this.queuedEffectCount++,i.add(e))}flush(){for(;this.queuedEffectCount>0;)for(let[e,t]of this.queues)e===null?this.flushQueue(t):e.run(()=>this.flushQueue(t))}flushQueue(e){for(let t of e)e.delete(t),this.queuedEffectCount--,t.run()}};function qo(n){return!!n&&typeof n.then=="function"}function m_(n){return!!n&&typeof n.subscribe=="function"}var nC=new Ne("");var g_=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=qe(nC,{optional:!0})??[];injector=qe(Es);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Wc(this.injector,r);if(qo(s))t.push(s);else if(m_(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),iC=new Ne("");function rC(){ju(()=>{throw new Te(600,!1)})}function sC(n){return n.isBoundToModule}var oC=10;var ko=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=qe(fE);afterRenderManager=qe(TE);zonelessEnabled=qe(uy);rootEffectScheduler=qe(tC);dirtyFlags=0;tracingSnapshot=null;externalTestViews=new Set;afterTick=new dn;get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];isStable=qe(As).hasPendingTasks.pipe(hn(t=>!t));constructor(){qe(Xc,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=qe(ei);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=Es.NULL){yt(10);let s=t instanceof n_;if(!this._injector.get(g_).done){let f="";throw new Te(405,f)}let a;s?a=t:a=this._injector.get(Qf).resolveComponentFactory(t),this.componentTypes.push(a.componentType);let c=sC(a)?void 0:this._injector.get(kc),l=i||a.selector,u=a.create(r,[],l,c),d=u.location.nativeElement,h=u.injector.get(p_,null);return h?.registerApplication(d),u.onDestroy(()=>{this.detachView(u.hostView),hc(this.components,u),h?.unregisterApplication(d)}),this._loadComponent(u),yt(11,u),u}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){yt(12),this.tracingSnapshot!==null?this.tracingSnapshot.run(Sy.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw new Te(101,!1);let t=Ze(null);try{this._runningTick=!0,this.synchronize()}catch(i){this.internalErrorHandler(i)}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Ze(t),this.afterTick.next(),yt(13)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ss,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<oC;)yt(14),this.synchronizeOnce(),yt(15)}synchronizeOnce(){if(this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush()),this.dirtyFlags&7){let t=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i,notifyErrorHandler:r}of this.allViews)aC(i,r,t,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}else this._rendererFactory?.begin?.(),this._rendererFactory?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>$c(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;hc(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView),this.tick(),this.components.push(t),this._injector.get(iC,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>hc(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Te(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function hc(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function aC(n,e,t,i){if(!t&&!$c(n))return;Zy(n,e,t&&!i?0:1)}function cC(n,e,t,i){return Or(n,wf(),t)?e+Ao(t)+i:Ei}function lC(n,e,t,i,r,s){let o=OM(),a=Zw(n,o,t,r);return zv(2),a?e+Ao(t)+i+Ao(r)+s:Ei}function cc(n,e){return n<<17|e<<2}function Fr(n){return n>>17&32767}function uC(n){return(n&2)==2}function dC(n,e){return n&131071|e<<17}function of(n){return n|2}function ws(n){return(n&131068)>>2}function _d(n,e){return n&-131069|e<<2}function fC(n){return(n&1)===1}function af(n){return n|1}function hC(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=Fr(o),c=ws(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Bo(d,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let h=Fr(n[a+1]);n[i+1]=cc(h,a),h!==0&&(n[h+1]=_d(n[h+1],i)),n[a+1]=dC(n[a+1],i)}else n[i+1]=cc(a,0),a!==0&&(n[a+1]=_d(n[a+1],i)),a=i;else n[i+1]=cc(c,0),a===0?a=i:n[c+1]=_d(n[c+1],i),c=i;l&&(n[i+1]=of(n[i+1])),Kg(n,u,i,!0),Kg(n,u,i,!1),pC(e,u,n,i,s),o=cc(a,c),s?e.classBindings=o:e.styleBindings=o}function pC(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Bo(s,e)>=0&&(t[i+1]=af(t[i+1]))}function Kg(n,e,t,i){let r=n[t+1],s=e===null,o=i?Fr(r):ws(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];mC(c,e)&&(a=!0,n[o+1]=i?af(l):of(l)),o=i?Fr(l):ws(l)}a&&(n[t+1]=i?of(r):af(r))}function mC(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Bo(n,e)>=0:!1}function Ye(n,e,t){let i=ft(),r=wf();if(Or(i,r,e)){let s=on(),o=Yv();By(s,o,i,n,e,i[Gt],t,!1)}return Ye}function Qg(n,e,t,i,r){Wf(e,n,t,r?"class":"style",i)}function wi(n,e,t){return v_(n,e,t,!1),wi}function _t(n,e){return v_(n,e,null,!0),_t}function v_(n,e,t,i){let r=ft(),s=on(),o=zv(2);if(s.firstUpdatePass&&vC(s,n,o,i),e!==Ei&&Or(r,o,e)){let a=s.data[Ur()];MC(s,a,r,r[Gt],n,r[o+1]=EC(e,t),i,o)}}function gC(n,e){return e>=n.expandoStartIndex}function vC(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Ur()],o=gC(n,t);SC(s,i)&&e===null&&!o&&(e=!1),e=yC(r,s,e,i),hC(r,s,e,t,o,i)}}function yC(n,e,t,i){let r=VM(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=xd(null,n,e,t,i),t=Uo(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=xd(r,n,e,t,i),s===null){let c=_C(n,e,i);c!==void 0&&Array.isArray(c)&&(c=xd(null,n,e,c[1],i),c=Uo(c,e.attrs,i),xC(n,e,i,c))}else s=bC(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function _C(n,e,t){let i=t?e.classBindings:e.styleBindings;if(ws(i)!==0)return n[Fr(i)]}function xC(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[Fr(r)]=i}function bC(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=Uo(i,o,t)}return Uo(i,e.attrs,t)}function xd(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=Uo(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function Uo(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Jb(n,o,t?!0:e[++s]))}return n===void 0?null:n}function MC(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=fC(l)?ev(c,e,t,r,ws(l),o):void 0;if(!Vc(u)){Vc(s)||uC(l)&&(s=ev(c,null,t,r,a,o));let d=Nv(Ur(),t);DS(i,o,d,r,s)}}function ev(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,d=u===null,h=t[r+1];h===Ei&&(h=d?An:void 0);let f=d?ld(h,i):u===i?h:void 0;if(l&&!Vc(f)&&(f=ld(c,i)),Vc(f)&&(a=f,o))return a;let g=n[r+1];r=o?Fr(g):ws(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=ld(c,i))}return a}function Vc(n){return n!==void 0}function EC(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=In(Dy(n)))),n}function SC(n,e){return(n.flags&(e?8:16))!==0}function V(n,e,t,i){let r=ft(),s=on(),o=Nn+n,a=r[Gt],c=s.firstCreatePass?s_(o,s,r,e,Hy,Uv(),t,i):s.data[o],l=wC(s,r,c,a,e,n);r[o]=l;let u=xf(c);return Ho(c,!0),Ry(a,l,c),!zy(c)&&Af()&&Xf(s,r,l,c),(SM()===0||u)&&Go(l,r),wM(),u&&(Gf(s,r,c),Ty(s,c,r)),i!==null&&Vy(r,c),V}function G(){let n=Pn();Bv()?RM():(n=n.parent,Ho(n,!1));let e=n;DM(e)&&AM(),CM();let t=on();return t.firstCreatePass&&o_(t,e),e.classesWithoutHost!=null&&WM(e)&&Qg(t,e,ft(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&jM(e)&&Qg(t,e,ft(),e.stylesWithoutHost,!1),G}function ve(n,e,t,i){return V(n,e,t,i),G(),ve}var wC=(n,e,t,i,r,s)=>(If(!0),Ay(i,r,Zv()));function On(){return ft()}var Bc="en-US";var CC=Bc;function TC(n){typeof n=="string"&&(CC=n.toLowerCase().replace(/_/g,"-"))}function tv(n,e,t){return function i(r){if(r===Function)return t;let s=Ts(n)?ri(n.index,e):e;Zf(s,5);let o=e[mn],a=nv(e,o,t,r),c=i.__ngNextListenerFn__;for(;c;)a=nv(e,o,c,r)&&a,c=c.__ngNextListenerFn__;return a}}function nv(n,e,t,i){let r=Ze(null);try{return yt(6,e,t),t(i)!==!1}catch(s){return DC(n,s),!1}finally{yt(7,e,t),Ze(r)}}function DC(n,e){let t=n[bs],i=t?t.get(vi,null):null;i&&i.handleError(e)}function iv(n,e,t,i,r,s){let o=e[t],a=e[Ge],l=a.data[t].outputs[i],u=o[l],d=a.firstCreatePass?Sf(a):null,h=Ef(e),f=u.subscribe(s),g=h.length;h.push(s,f),d&&d.push(r,n.index,g,-(g+1))}function Ie(n,e,t,i){let r=ft(),s=on(),o=Pn();return y_(s,r,r[Gt],o,n,e,i),Ie}function AC(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Mc],c=r[s+2];return a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function y_(n,e,t,i,r,s,o){let a=xf(i),l=n.firstCreatePass?Sf(n):null,u=Ef(e),d=!0;if(i.type&3||o){let h=bi(i,e),f=o?o(h):h,g=u.length,v=o?p=>o(ii(p[i.index])):i.index,m=null;if(!o&&a&&(m=AC(n,e,r,i.index)),m!==null){let p=m.__ngLastListenerFn__||m;p.__ngNextListenerFn__=s,m.__ngLastListenerFn__=s,d=!1}else{s=tv(i,e,s),AE(e,f,r,s);let p=t.listen(f,r,s);u.push(s,p),l&&l.push(r,v,g,g+1)}}else s=tv(i,e,s);if(d){let h=i.outputs?.[r],f=i.hostDirectiveOutputs?.[r];if(f&&f.length)for(let g=0;g<f.length;g+=2){let v=f[g],m=f[g+1];iv(i,e,v,m,r,s)}if(h&&h.length)for(let g of h)iv(i,e,g,r,r,s)}}function qt(n=1){return HM(n)}function Zi(n,e,t){Iw(n,e,t)}function Ji(n){let e=ft(),t=on(),i=Gv();Cf(i+1);let r=eh(t,i);if(n.dirty&&_M(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=Pw(e,i);n.reset(s,pE),n.notifyOnChanges()}return!0}return!1}function Ki(){return Dw(ft(),Gv())}function __(n){let e=NM();return Ov(e,Nn+n)}function ee(n,e=""){let t=ft(),i=on(),r=n+Nn,s=i.firstCreatePass?Kf(i,r,1,e,null):i.data[r],o=IC(i,t,s,e,n);t[r]=o,Af()&&Xf(i,t,o,s),Ho(s,!1)}var IC=(n,e,t,i,r)=>(If(!0),FE(e[Gt],i));function Xt(n){return Ct("",n,""),Xt}function Ct(n,e,t){let i=ft(),r=cC(i,n,e,t);return r!==Ei&&x_(i,Ur(),r),Ct}function th(n,e,t,i,r){let s=ft(),o=lC(s,n,e,t,i,r);return o!==Ei&&x_(s,Ur(),o),th}function x_(n,e,t){let i=Nv(e,n);LE(n[Gt],i,t)}function Qi(n,e,t){my(e)&&(e=e());let i=ft(),r=wf();if(Or(i,r,e)){let s=on(),o=Yv();By(s,o,i,n,e,i[Gt],t,!1)}return Qi}function Br(n,e){let t=my(n);return t&&n.set(e),t}function er(n,e){let t=ft(),i=on(),r=Pn();return y_(i,t,t[Gt],r,n,e),er}function RC(n,e,t){let i=on();if(i.firstCreatePass){let r=ni(n);cf(t,i.data,i.blueprint,r,!0),cf(e,i.data,i.blueprint,r,!1)}}function cf(n,e,t,i,r){if(n=sn(n),Array.isArray(n))for(let s=0;s<n.length;s++)cf(n[s],e,t,i,r);else{let s=on(),o=ft(),a=Pn(),c=xs(n)?n:sn(n.provide),l=Sv(n),u=a.providerIndexes&1048575,d=a.directiveStart,h=a.providerIndexes>>20;if(xs(n)||!n.multi){let f=new Nr(l,r,Se),g=Md(c,e,r?u:u+h,d);g===-1?(Od(Ic(a,o),s,c),bd(s,n,e.length),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(f),o.push(f)):(t[g]=f,o[g]=f)}else{let f=Md(c,e,u+h,d),g=Md(c,e,u,u+h),v=f>=0&&t[f],m=g>=0&&t[g];if(r&&!m||!r&&!v){Od(Ic(a,o),s,c);let p=OC(r?PC:NC,t.length,r,i,l);!r&&m&&(t[g].providerFactory=p),bd(s,n,e.length,0),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=b_(t[r?g:f],l,!r&&i);bd(s,n,f>-1?f:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function bd(n,e,t,i){let r=xs(e),s=sM(e);if(r||s){let c=(s?sn(e.useClass):e).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=l.indexOf(t);u===-1?l.push(t,[i,c]):l[u+1].push(i,c)}else l.push(t,c)}}}function b_(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function Md(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function NC(n,e,t,i,r){return lf(this.multi,[])}function PC(n,e,t,i,r){let s=this.multi,o;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Oo(i,i[Ge],this.providerFactory.index,r);o=c.slice(0,a),lf(s,o);for(let l=a;l<c.length;l++)o.push(c[l])}else o=[],lf(s,o);return o}function lf(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function OC(n,e,t,i,r){let s=new Nr(n,t,Se);return s.multi=[],s.index=e,s.componentProviders=0,b_(s,r,i&&!t),s}function Xo(n,e=[]){return t=>{t.providersResolver=(i,r)=>RC(i,r?r(n):n,e)}}function FC(n,e){let t=n[e];return t===Ei?void 0:t}function LC(n,e,t,i,r,s){let o=e+t;return Or(n,o,r)?Yw(n,o+1,s?i.call(s,r):i(r)):FC(n,o+1)}function Yo(n,e){let t=on(),i,r=n+Nn;t.firstCreatePass?(i=kC(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=Cr(i.type,!0)),o,a=pn(Se);try{let c=Ac(!1),l=s();return Ac(c),yM(t,ft(),r,l),l}finally{pn(a)}}function kC(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function Zo(n,e,t){let i=n+Nn,r=ft(),s=Ov(r,i);return UC(r,i)?LC(r,PM(),e,s.transform,t,s):s.transform(t)}function UC(n,e){return n[Ge].data[e].pure}var VC=(()=>{class n{zone=qe(Jt);changeDetectionScheduler=qe(Fo);applicationRef=qe(ko);_onMicrotaskEmptySubscription;initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function BC({ngZoneFactory:n,ignoreChangesOutsideZone:e,scheduleInRootZone:t}){return n??=()=>new Jt(wt(dt({},HC()),{scheduleInRootZone:t})),[{provide:Jt,useFactory:n},{provide:_c,multi:!0,useFactory:()=>{let i=qe(VC,{optional:!0});return()=>i.initialize()}},{provide:_c,multi:!0,useFactory:()=>{let i=qe(zC);return()=>{i.initialize()}}},e===!0?{provide:dy,useValue:!0}:[],{provide:fy,useValue:t??ly}]}function HC(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var zC=(()=>{class n{subscription=new rn;initialized=!1;zone=qe(Jt);pendingTasks=qe(As);initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{Jt.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{Jt.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var GC=(()=>{class n{appRef=qe(ko);taskService=qe(As);ngZone=qe(Jt);zonelessEnabled=qe(uy);tracing=qe(Xc,{optional:!0});disableScheduling=qe(dy,{optional:!0})??!1;zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new rn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Nc):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(qe(fy,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof Vd||!this.zoneIsDefined)}notify(t){if(!this.zonelessEnabled&&t===5)return;let i=!1;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2,i=!0;break}case 12:{this.appRef.dirtyFlags|=16,i=!0;break}case 13:{this.appRef.dirtyFlags|=2,i=!0;break}case 11:{i=!0;break}default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick(i))return;let r=this.useMicrotaskScheduler?Lg:hy;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(t){return!(this.disableScheduling&&!t||this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Nc+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){throw this.taskService.remove(t),i}finally{this.cleanup()}this.useMicrotaskScheduler=!0,Lg(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function WC(){return typeof $localize<"u"&&$localize.locale||Bc}var nh=new Ne("",{providedIn:"root",factory:()=>qe(nh,We.Optional|We.SkipSelf)||WC()});var uf=new Ne(""),jC=new Ne("");function wo(n){return!n.moduleRef}function $C(n){let e=wo(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Jt);return t.run(()=>{wo(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(vi,null),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:s=>{i.handleError(s)}})}),wo(n)){let s=()=>e.destroy(),o=n.platformInjector.get(uf);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(uf);o.add(s),n.moduleRef.onDestroy(()=>{hc(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return XC(i,t,()=>{let s=e.get(g_);return s.runInitializers(),s.donePromise.then(()=>{let o=e.get(nh,Bc);if(TC(o||Bc),!e.get(jC,!0))return wo(n)?e.get(ko):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(wo(n)){let c=e.get(ko);return n.rootComponent!==void 0&&c.bootstrap(n.rootComponent),c}else return qC(n.moduleRef,n.allPlatformModules),n.moduleRef})})})}function qC(n,e){let t=n.injector.get(ko);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>t.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(t);else throw new Te(-403,!1);e.push(n)}function XC(n,e,t){try{let i=t();return qo(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n.handleError(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n.handleError(i)),i}}var pc=null;function YC(n=[],e){return Es.create({name:e,providers:[{provide:Gc,useValue:"platform"},{provide:uf,useValue:new Set([()=>pc=null])},...n]})}function ZC(n=[]){if(pc)return pc;let e=YC(n);return pc=e,rC(),JC(e),e}function JC(n){let e=n.get(kf,null);Wc(n,()=>{e?.forEach(t=>t())})}var ih=(()=>{class n{static __NG_ELEMENT_ID__=KC}return n})();function KC(n){return QC(Pn(),ft(),(n&16)===16)}function QC(n,e,t){if(Ts(n)&&!t){let i=ri(n.index,e);return new Lo(i,i)}else if(n.type&175){let i=e[ti];return new Lo(i,e)}return null}var df=class{constructor(){}supports(e){return h_(e)}create(e){return new ff(e)}},eT=(n,e)=>e,ff=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(e){this._trackByFn=e||eT}forEachItem(e){let t;for(t=this._itHead;t!==null;t=t._next)e(t)}forEachOperation(e){let t=this._itHead,i=this._removalsHead,r=0,s=null;for(;t||i;){let o=!i||t&&t.currentIndex<rv(i,r,s)?t:i,a=rv(o,r,s),c=o.currentIndex;if(o===i)r--,i=i._nextRemoved;else if(t=t._next,o.previousIndex==null)r++;else{s||(s=[]);let l=a-r,u=c-r;if(l!=u){for(let h=0;h<l;h++){let f=h<s.length?s[h]:s[h]=0,g=f+h;u<=g&&g<l&&(s[h]=f+1)}let d=o.previousIndex;s[d]=u-l}}a!==c&&e(o,a,c)}}forEachPreviousItem(e){let t;for(t=this._previousItHead;t!==null;t=t._nextPrevious)e(t)}forEachAddedItem(e){let t;for(t=this._additionsHead;t!==null;t=t._nextAdded)e(t)}forEachMovedItem(e){let t;for(t=this._movesHead;t!==null;t=t._nextMoved)e(t)}forEachRemovedItem(e){let t;for(t=this._removalsHead;t!==null;t=t._nextRemoved)e(t)}forEachIdentityChange(e){let t;for(t=this._identityChangesHead;t!==null;t=t._nextIdentityChange)e(t)}diff(e){if(e==null&&(e=[]),!h_(e))throw new Te(900,!1);return this.check(e)?this:null}onDestroy(){}check(e){this._reset();let t=this._itHead,i=!1,r,s,o;if(Array.isArray(e)){this.length=e.length;for(let a=0;a<this.length;a++)s=e[a],o=this._trackByFn(a,s),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,s,o,a),i=!0):(i&&(t=this._verifyReinsertion(t,s,o,a)),Object.is(t.item,s)||this._addIdentityChange(t,s)),t=t._next}else r=0,qw(e,a=>{o=this._trackByFn(r,a),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,a,o,r),i=!0):(i&&(t=this._verifyReinsertion(t,a,o,r)),Object.is(t.item,a)||this._addIdentityChange(t,a)),t=t._next,r++}),this.length=r;return this._truncate(t),this.collection=e,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let e;for(e=this._previousItHead=this._itHead;e!==null;e=e._next)e._nextPrevious=e._next;for(e=this._additionsHead;e!==null;e=e._nextAdded)e.previousIndex=e.currentIndex;for(this._additionsHead=this._additionsTail=null,e=this._movesHead;e!==null;e=e._nextMoved)e.previousIndex=e.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(e,t,i,r){let s;return e===null?s=this._itTail:(s=e._prev,this._remove(e)),e=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._reinsertAfter(e,s,r)):(e=this._linkedRecords===null?null:this._linkedRecords.get(i,r),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._moveAfter(e,s,r)):e=this._addAfter(new hf(t,i),s,r)),e}_verifyReinsertion(e,t,i,r){let s=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return s!==null?e=this._reinsertAfter(s,e._prev,r):e.currentIndex!=r&&(e.currentIndex=r,this._addToMoves(e,r)),e}_truncate(e){for(;e!==null;){let t=e._next;this._addToRemovals(this._unlink(e)),e=t}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(e,t,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(e);let r=e._prevRemoved,s=e._nextRemoved;return r===null?this._removalsHead=s:r._nextRemoved=s,s===null?this._removalsTail=r:s._prevRemoved=r,this._insertAfter(e,t,i),this._addToMoves(e,i),e}_moveAfter(e,t,i){return this._unlink(e),this._insertAfter(e,t,i),this._addToMoves(e,i),e}_addAfter(e,t,i){return this._insertAfter(e,t,i),this._additionsTail===null?this._additionsTail=this._additionsHead=e:this._additionsTail=this._additionsTail._nextAdded=e,e}_insertAfter(e,t,i){let r=t===null?this._itHead:t._next;return e._next=r,e._prev=t,r===null?this._itTail=e:r._prev=e,t===null?this._itHead=e:t._next=e,this._linkedRecords===null&&(this._linkedRecords=new Hc),this._linkedRecords.put(e),e.currentIndex=i,e}_remove(e){return this._addToRemovals(this._unlink(e))}_unlink(e){this._linkedRecords!==null&&this._linkedRecords.remove(e);let t=e._prev,i=e._next;return t===null?this._itHead=i:t._next=i,i===null?this._itTail=t:i._prev=t,e}_addToMoves(e,t){return e.previousIndex===t||(this._movesTail===null?this._movesTail=this._movesHead=e:this._movesTail=this._movesTail._nextMoved=e),e}_addToRemovals(e){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Hc),this._unlinkedRecords.put(e),e.currentIndex=null,e._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=e,e._prevRemoved=null):(e._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=e),e}_addIdentityChange(e,t){return e.item=t,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=e:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=e,e}},hf=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(e,t){this.item=e,this.trackById=t}},pf=class{_head=null;_tail=null;add(e){this._head===null?(this._head=this._tail=e,e._nextDup=null,e._prevDup=null):(this._tail._nextDup=e,e._prevDup=this._tail,e._nextDup=null,this._tail=e)}get(e,t){let i;for(i=this._head;i!==null;i=i._nextDup)if((t===null||t<=i.currentIndex)&&Object.is(i.trackById,e))return i;return null}remove(e){let t=e._prevDup,i=e._nextDup;return t===null?this._head=i:t._nextDup=i,i===null?this._tail=t:i._prevDup=t,this._head===null}},Hc=class{map=new Map;put(e){let t=e.trackById,i=this.map.get(t);i||(i=new pf,this.map.set(t,i)),i.add(e)}get(e,t){let i=e,r=this.map.get(i);return r?r.get(e,t):null}remove(e){let t=e.trackById;return this.map.get(t).remove(e)&&this.map.delete(t),e}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function rv(n,e,t){let i=n.previousIndex;if(i===null)return i;let r=0;return t&&i<t.length&&(r=t[i]),i+e+r}function sv(){return new rh([new df])}var rh=(()=>{class n{factories;static \u0275prov=tt({token:n,providedIn:"root",factory:sv});constructor(t){this.factories=t}static create(t,i){if(i!=null){let r=i.factories.slice();t=t.concat(r)}return new n(t)}static extend(t){return{provide:n,useFactory:i=>n.create(t,i||sv()),deps:[[n,new qb,new $b]]}}find(t){let i=this.factories.find(r=>r.supports(t));if(i!=null)return i;throw new Te(901,!1)}}return n})();function M_(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;yt(8);try{let s=r?.injector??ZC(i),o=[BC({}),{provide:Fo,useExisting:GC},...t||[]],a=new Uc({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return $C({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{yt(9)}}function E_(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function Ci(n){return Yu(n)}function Jo(n,e){return Wu(n,e?.equal)}var ov=class{[Hn];constructor(e){this[Hn]=e}destroy(){this[Hn].destroy()}};var Fn=new Ne("");var S_=null;function tr(){return S_}function sh(n){S_??=n}var Ko=class{};var oh=/\s+/,w_=[],Ti=(()=>{class n{_ngEl;_renderer;initialClasses=w_;rawClass;stateMap=new Map;constructor(t,i){this._ngEl=t,this._renderer=i}set klass(t){this.initialClasses=t!=null?t.trim().split(oh):w_}set ngClass(t){this.rawClass=typeof t=="string"?t.trim().split(oh):t}ngDoCheck(){for(let i of this.initialClasses)this._updateState(i,!0);let t=this.rawClass;if(Array.isArray(t)||t instanceof Set)for(let i of t)this._updateState(i,!0);else if(t!=null)for(let i of Object.keys(t))this._updateState(i,!!t[i]);this._applyStateDiff()}_updateState(t,i){let r=this.stateMap.get(t);r!==void 0?(r.enabled!==i&&(r.changed=!0,r.enabled=i),r.touched=!0):this.stateMap.set(t,{enabled:i,changed:!0,touched:!0})}_applyStateDiff(){for(let t of this.stateMap){let i=t[0],r=t[1];r.changed?(this._toggleClass(i,r.enabled),r.changed=!1):r.touched||(r.enabled&&this._toggleClass(i,!1),this.stateMap.delete(i)),r.touched=!1}}_toggleClass(t,i){t=t.trim(),t.length>0&&t.split(oh).forEach(r=>{i?this._renderer.addClass(this._ngEl.nativeElement,r):this._renderer.removeClass(this._ngEl.nativeElement,r)})}static \u0275fac=function(i){return new(i||n)(Se(Mi),Se($o))};static \u0275dir=gn({type:n,selectors:[["","ngClass",""]],inputs:{klass:[0,"class","klass"],ngClass:"ngClass"}})}return n})();var Jc=class{$implicit;ngForOf;index;count;constructor(e,t,i,r){this.$implicit=e,this.ngForOf=t,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Ln=(()=>{class n{_viewContainer;_template;_differs;set ngForOf(t){this._ngForOf=t,this._ngForOfDirty=!0}set ngForTrackBy(t){this._trackByFn=t}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(t,i,r){this._viewContainer=t,this._template=i,this._differs=r}set ngForTemplate(t){t&&(this._template=t)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let t=this._ngForOf;!this._differ&&t&&(this._differ=this._differs.find(t).create(this.ngForTrackBy))}if(this._differ){let t=this._differ.diff(this._ngForOf);t&&this._applyChanges(t)}}_applyChanges(t){let i=this._viewContainer;t.forEachOperation((r,s,o)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Jc(r.item,this._ngForOf,-1,-1),o===null?void 0:o);else if(o==null)i.remove(s===null?void 0:s);else if(s!==null){let a=i.get(s);i.move(a,o),C_(a,r)}});for(let r=0,s=i.length;r<s;r++){let a=i.get(r).context;a.index=r,a.count=s,a.ngForOf=this._ngForOf}t.forEachIdentityChange(r=>{let s=i.get(r.currentIndex);C_(s,r)})}static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(Se(Rs),Se(Pr),Se(rh))};static \u0275dir=gn({type:n,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return n})();function C_(n,e){n.context.$implicit=e.item}var Di=(()=>{class n{_viewContainer;_context=new Kc;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(t,i){this._viewContainer=t,this._thenTemplateRef=i}set ngIf(t){this._context.$implicit=this._context.ngIf=t,this._updateView()}set ngIfThen(t){T_(t,!1),this._thenTemplateRef=t,this._thenViewRef=null,this._updateView()}set ngIfElse(t){T_(t,!1),this._elseTemplateRef=t,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(Se(Rs),Se(Pr))};static \u0275dir=gn({type:n,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return n})(),Kc=class{$implicit=null;ngIf=null};function T_(n,e){if(n&&!n.createEmbeddedView)throw new Te(2020,!1)}function tT(n,e){return new Te(2100,!1)}var Qo=(()=>{class n{transform(t){if(t==null)return null;if(typeof t!="string")throw tT(n,t);return t.toUpperCase()}static \u0275fac=function(i){return new(i||n)};static \u0275pipe=u_({name:"uppercase",type:n,pure:!0})}return n})();var Rt=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Vr({type:n});static \u0275inj=kr({})}return n})();function ea(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var ah="browser",D_="server";function Qc(n){return n===D_}var Hr=class{};var nl=new Ne(""),dh=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(r=>{r.manager=this}),this._plugins=t.slice().reverse()}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Te(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(Xe(nl),Xe(Jt))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})(),ta=class{_doc;constructor(e){this._doc=e}manager},el="ng-app-id";function A_(n){for(let e of n)e.remove()}function I_(n,e){let t=e.createElement("style");return t.textContent=n,t}function nT(n,e,t,i){let r=n.head?.querySelectorAll(`style[${el}="${e}"],link[${el}="${e}"]`);if(r)for(let s of r)s.removeAttribute(el),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function lh(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var fh=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;isServer;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,this.isServer=Qc(s),nT(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,I_);i?.forEach(r=>this.addUsage(r,this.external,lh))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(A_(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])A_(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,I_(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,lh(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),this.isServer&&i.setAttribute(el,this.appId),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(Xe(Fn),Xe(Lf),Xe(Uf,8),Xe(Wo))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})(),ch={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},hh=/%COMP%/g;var N_="%COMP%",iT=`_nghost-${N_}`,rT=`_ngcontent-${N_}`,sT=!0,oT=new Ne("",{providedIn:"root",factory:()=>sT});function aT(n){return rT.replace(hh,n)}function cT(n){return iT.replace(hh,n)}function P_(n,e){return e.map(t=>t.replace(hh,n))}var ph=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;platformId;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;platformIsServer;constructor(t,i,r,s,o,a,c,l=null,u=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.platformId=a,this.ngZone=c,this.nonce=l,this.tracingService=u,this.platformIsServer=Qc(a),this.defaultRenderer=new na(t,o,c,this.platformIsServer,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;this.platformIsServer&&i.encapsulation===si.ShadowDom&&(i=wt(dt({},i),{encapsulation:si.Emulated}));let r=this.getOrCreateRenderer(t,i);return r instanceof tl?r.applyToHost(t):r instanceof ia&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.platformIsServer,h=this.tracingService;switch(i.encapsulation){case si.Emulated:s=new tl(c,l,i,this.appId,u,o,a,d,h);break;case si.ShadowDom:return new uh(c,l,t,i,o,a,this.nonce,d,h);default:s=new ia(c,l,i,u,o,a,d,h);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(Xe(dh),Xe(fh),Xe(Lf),Xe(oT),Xe(Fn),Xe(Wo),Xe(Jt),Xe(Uf),Xe(Xc,8))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})(),na=class{eventManager;doc;ngZone;platformIsServer;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r,s){this.eventManager=e,this.doc=t,this.ngZone=i,this.platformIsServer=r,this.tracingService=s}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(ch[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(R_(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(R_(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Te(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=ch[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=ch[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(yi.DashCase|yi.Important)?e.style.setProperty(t,i,r&yi.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&yi.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=tr().getGlobalEventTarget(this.doc,e),!e))throw new Te(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;(this.platformIsServer?this.ngZone.runGuarded(()=>e(t)):e(t))===!1&&t.preventDefault()}}};function R_(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var uh=class extends na{sharedStylesHost;hostEl;shadowRoot;constructor(e,t,i,r,s,o,a,c,l){super(e,s,o,c,l),this.sharedStylesHost=t,this.hostEl=i,this.shadowRoot=i.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let u=r.styles;u=P_(r.id,u);for(let h of u){let f=document.createElement("style");a&&f.setAttribute("nonce",a),f.textContent=h,this.shadowRoot.appendChild(f)}let d=r.getExternalStyles?.();if(d)for(let h of d){let f=lh(h,s);a&&f.setAttribute("nonce",a),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},ia=class extends na{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c,l){super(e,s,o,a,c),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let u=i.styles;this.styles=l?P_(l,u):u,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},tl=class extends ia{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c,l){let u=r+"-"+i.id;super(e,t,i,s,o,a,c,l,u),this.contentAttr=aT(u),this.hostAttr=cT(u)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var il=class n extends Ko{supportsDOMEvents=!0;static makeCurrent(){sh(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=lT();return t==null?null:uT(t)}resetBaseElement(){ra=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return ea(document.cookie,e)}},ra=null;function lT(){return ra=ra||document.head.querySelector("base"),ra?ra.getAttribute("href"):null}function uT(n){return new URL(n,document.baseURI).pathname}var dT=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})(),F_=(()=>{class n extends ta{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(Xe(Fn))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})(),O_=["alt","control","meta","shift"],fT={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},hT={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},L_=(()=>{class n extends ta{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>tr().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),O_.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=fT[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),O_.forEach(o=>{if(o!==r){let a=hT[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(Xe(Fn))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})();function mh(n,e,t){return M_(dt({rootComponent:n,platformRef:t?.platformRef},pT(e)))}function pT(n){return{appProviders:[..._T,...n?.providers??[]],platformProviders:yT}}function mT(){il.makeCurrent()}function gT(){return new vi}function vT(){return My(document),document}var yT=[{provide:Wo,useValue:ah},{provide:kf,useValue:mT,multi:!0},{provide:Fn,useFactory:vT}];var _T=[{provide:Gc,useValue:"root"},{provide:vi,useFactory:gT},{provide:nl,useClass:F_,multi:!0,deps:[Fn]},{provide:nl,useClass:L_,multi:!0,deps:[Fn]},ph,fh,dh,{provide:Ss,useExisting:ph},{provide:Hr,useClass:dT},[]];var Ps=class{},sa=class{},nr=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(e){e?typeof e=="string"?this.lazyInit=()=>{this.headers=new Map,e.split(`
`).forEach(t=>{let i=t.indexOf(":");if(i>0){let r=t.slice(0,i),s=t.slice(i+1).trim();this.addHeaderEntry(r,s)}})}:typeof Headers<"u"&&e instanceof Headers?(this.headers=new Map,e.forEach((t,i)=>{this.addHeaderEntry(i,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(e).forEach(([t,i])=>{this.setHeaderEntries(t,i)})}:this.headers=new Map}has(e){return this.init(),this.headers.has(e.toLowerCase())}get(e){this.init();let t=this.headers.get(e.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(e){return this.init(),this.headers.get(e.toLowerCase())||null}append(e,t){return this.clone({name:e,value:t,op:"a"})}set(e,t){return this.clone({name:e,value:t,op:"s"})}delete(e,t){return this.clone({name:e,value:t,op:"d"})}maybeSetNormalizedName(e,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,e)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(e=>this.applyUpdate(e)),this.lazyUpdate=null))}copyFrom(e){e.init(),Array.from(e.headers.keys()).forEach(t=>{this.headers.set(t,e.headers.get(t)),this.normalizedNames.set(t,e.normalizedNames.get(t))})}clone(e){let t=new n;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([e]),t}applyUpdate(e){let t=e.name.toLowerCase();switch(e.op){case"a":case"s":let i=e.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(e.name,t);let r=(e.op==="a"?this.headers.get(t):void 0)||[];r.push(...i),this.headers.set(t,r);break;case"d":let s=e.value;if(!s)this.headers.delete(t),this.normalizedNames.delete(t);else{let o=this.headers.get(t);if(!o)return;o=o.filter(a=>s.indexOf(a)===-1),o.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,o)}break}}addHeaderEntry(e,t){let i=e.toLowerCase();this.maybeSetNormalizedName(e,i),this.headers.has(i)?this.headers.get(i).push(t):this.headers.set(i,[t])}setHeaderEntries(e,t){let i=(Array.isArray(t)?t:[t]).map(s=>s.toString()),r=e.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(e,r)}forEach(e){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>e(this.normalizedNames.get(t),this.headers.get(t)))}};var sl=class{encodeKey(e){return k_(e)}encodeValue(e){return k_(e)}decodeKey(e){return decodeURIComponent(e)}decodeValue(e){return decodeURIComponent(e)}};function xT(n,e){let t=new Map;return n.length>0&&n.replace(/^\?/,"").split("&").forEach(r=>{let s=r.indexOf("="),[o,a]=s==-1?[e.decodeKey(r),""]:[e.decodeKey(r.slice(0,s)),e.decodeValue(r.slice(s+1))],c=t.get(o)||[];c.push(a),t.set(o,c)}),t}var bT=/%(\d[a-f0-9])/gi,MT={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function k_(n){return encodeURIComponent(n).replace(bT,(e,t)=>MT[t]??e)}function rl(n){return`${n}`}var Ai=class n{map;encoder;updates=null;cloneFrom=null;constructor(e={}){if(this.encoder=e.encoder||new sl,e.fromString){if(e.fromObject)throw new Te(2805,!1);this.map=xT(e.fromString,this.encoder)}else e.fromObject?(this.map=new Map,Object.keys(e.fromObject).forEach(t=>{let i=e.fromObject[t],r=Array.isArray(i)?i.map(rl):[rl(i)];this.map.set(t,r)})):this.map=null}has(e){return this.init(),this.map.has(e)}get(e){this.init();let t=this.map.get(e);return t?t[0]:null}getAll(e){return this.init(),this.map.get(e)||null}keys(){return this.init(),Array.from(this.map.keys())}append(e,t){return this.clone({param:e,value:t,op:"a"})}appendAll(e){let t=[];return Object.keys(e).forEach(i=>{let r=e[i];Array.isArray(r)?r.forEach(s=>{t.push({param:i,value:s,op:"a"})}):t.push({param:i,value:r,op:"a"})}),this.clone(t)}set(e,t){return this.clone({param:e,value:t,op:"s"})}delete(e,t){return this.clone({param:e,value:t,op:"d"})}toString(){return this.init(),this.keys().map(e=>{let t=this.encoder.encodeKey(e);return this.map.get(e).map(i=>t+"="+this.encoder.encodeValue(i)).join("&")}).filter(e=>e!=="").join("&")}clone(e){let t=new n({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(e),t}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(e=>this.map.set(e,this.cloneFrom.map.get(e))),this.updates.forEach(e=>{switch(e.op){case"a":case"s":let t=(e.op==="a"?this.map.get(e.param):void 0)||[];t.push(rl(e.value)),this.map.set(e.param,t);break;case"d":if(e.value!==void 0){let i=this.map.get(e.param)||[],r=i.indexOf(rl(e.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(e.param,i):this.map.delete(e.param)}else{this.map.delete(e.param);break}}}),this.cloneFrom=this.updates=null)}};var ol=class{map=new Map;set(e,t){return this.map.set(e,t),this}get(e){return this.map.has(e)||this.map.set(e,e.defaultValue()),this.map.get(e)}delete(e){return this.map.delete(e),this}has(e){return this.map.has(e)}keys(){return this.map.keys()}};function ET(n){switch(n){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function U_(n){return typeof ArrayBuffer<"u"&&n instanceof ArrayBuffer}function V_(n){return typeof Blob<"u"&&n instanceof Blob}function B_(n){return typeof FormData<"u"&&n instanceof FormData}function ST(n){return typeof URLSearchParams<"u"&&n instanceof URLSearchParams}var H_="Content-Type",z_="Accept",G_="X-Request-URL",W_="text/plain",j_="application/json",wT=`${j_}, ${W_}, */*`,Ns=class n{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;responseType="json";method;params;urlWithParams;transferCache;constructor(e,t,i,r){this.url=t,this.method=e.toUpperCase();let s;if(ET(this.method)||r?(this.body=i!==void 0?i:null,s=r):s=i,s&&(this.reportProgress=!!s.reportProgress,this.withCredentials=!!s.withCredentials,s.responseType&&(this.responseType=s.responseType),s.headers&&(this.headers=s.headers),s.context&&(this.context=s.context),s.params&&(this.params=s.params),this.transferCache=s.transferCache),this.headers??=new nr,this.context??=new ol,!this.params)this.params=new Ai,this.urlWithParams=t;else{let o=this.params.toString();if(o.length===0)this.urlWithParams=t;else{let a=t.indexOf("?"),c=a===-1?"?":a<t.length-1?"&":"";this.urlWithParams=t+c+o}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||U_(this.body)||V_(this.body)||B_(this.body)||ST(this.body)?this.body:this.body instanceof Ai?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||B_(this.body)?null:V_(this.body)?this.body.type||null:U_(this.body)?null:typeof this.body=="string"?W_:this.body instanceof Ai?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?j_:null}clone(e={}){let t=e.method||this.method,i=e.url||this.url,r=e.responseType||this.responseType,s=e.transferCache??this.transferCache,o=e.body!==void 0?e.body:this.body,a=e.withCredentials??this.withCredentials,c=e.reportProgress??this.reportProgress,l=e.headers||this.headers,u=e.params||this.params,d=e.context??this.context;return e.setHeaders!==void 0&&(l=Object.keys(e.setHeaders).reduce((h,f)=>h.set(f,e.setHeaders[f]),l)),e.setParams&&(u=Object.keys(e.setParams).reduce((h,f)=>h.set(f,e.setParams[f]),u)),new n(t,i,o,{params:u,headers:l,context:d,reportProgress:c,responseType:r,withCredentials:a,transferCache:s})}},Gr=(function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n})(Gr||{}),Os=class{headers;status;statusText;url;ok;type;constructor(e,t=200,i="OK"){this.headers=e.headers||new nr,this.status=e.status!==void 0?e.status:t,this.statusText=e.statusText||i,this.url=e.url||null,this.ok=this.status>=200&&this.status<300}},al=class n extends Os{constructor(e={}){super(e)}type=Gr.ResponseHeader;clone(e={}){return new n({headers:e.headers||this.headers,status:e.status!==void 0?e.status:this.status,statusText:e.statusText||this.statusText,url:e.url||this.url||void 0})}},oa=class n extends Os{body;constructor(e={}){super(e),this.body=e.body!==void 0?e.body:null}type=Gr.Response;clone(e={}){return new n({body:e.body!==void 0?e.body:this.body,headers:e.headers||this.headers,status:e.status!==void 0?e.status:this.status,statusText:e.statusText||this.statusText,url:e.url||this.url||void 0})}},aa=class extends Os{name="HttpErrorResponse";message;error;ok=!1;constructor(e){super(e,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${e.url||"(unknown url)"}`:this.message=`Http failure response for ${e.url||"(unknown url)"}: ${e.status} ${e.statusText}`,this.error=e.error||null}},CT=200,TT=204;function gh(n,e){return{body:e,headers:n.headers,context:n.context,observe:n.observe,params:n.params,reportProgress:n.reportProgress,responseType:n.responseType,withCredentials:n.withCredentials,transferCache:n.transferCache}}var ll=(()=>{class n{handler;constructor(t){this.handler=t}request(t,i,r={}){let s;if(t instanceof Ns)s=t;else{let c;r.headers instanceof nr?c=r.headers:c=new nr(r.headers);let l;r.params&&(r.params instanceof Ai?l=r.params:l=new Ai({fromObject:r.params})),s=new Ns(t,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache})}let o=sc(s).pipe(od(c=>this.handler.handle(c)));if(t instanceof Ns||r.observe==="events")return o;let a=o.pipe(sd(c=>c instanceof oa));switch(r.observe||"body"){case"body":switch(s.responseType){case"arraybuffer":return a.pipe(hn(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new Te(2806,!1);return c.body}));case"blob":return a.pipe(hn(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new Te(2807,!1);return c.body}));case"text":return a.pipe(hn(c=>{if(c.body!==null&&typeof c.body!="string")throw new Te(2808,!1);return c.body}));default:return a.pipe(hn(c=>c.body))}case"response":return a;default:throw new Te(2809,!1)}}delete(t,i={}){return this.request("DELETE",t,i)}get(t,i={}){return this.request("GET",t,i)}head(t,i={}){return this.request("HEAD",t,i)}jsonp(t,i){return this.request("JSONP",t,{params:new Ai().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,i={}){return this.request("OPTIONS",t,i)}patch(t,i,r={}){return this.request("PATCH",t,gh(r,i))}post(t,i,r={}){return this.request("POST",t,gh(r,i))}put(t,i,r={}){return this.request("PUT",t,gh(r,i))}static \u0275fac=function(i){return new(i||n)(Xe(Ps))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})();var DT=new Ne("");function AT(n,e){return e(n)}function IT(n,e,t){return(i,r)=>Wc(t,()=>e(i,s=>n(s,r)))}var $_=new Ne(""),q_=new Ne(""),X_=new Ne("",{providedIn:"root",factory:()=>!0});var cl=(()=>{class n extends Ps{backend;injector;chain=null;pendingTasks=qe(As);contributeToStability=qe(X_);constructor(t,i){super(),this.backend=t,this.injector=i}handle(t){if(this.chain===null){let i=Array.from(new Set([...this.injector.get($_),...this.injector.get(q_,[])]));this.chain=i.reduceRight((r,s)=>IT(r,s,this.injector),AT)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(t,r=>this.backend.handle(r)).pipe(ad(()=>this.pendingTasks.remove(i)))}else return this.chain(t,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||n)(Xe(sa),Xe(ei))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})();var RT=/^\)\]\}',?\n/,NT=RegExp(`^${G_}:`,"m");function PT(n){return"responseURL"in n&&n.responseURL?n.responseURL:NT.test(n.getAllResponseHeaders())?n.getResponseHeader(G_):null}var vh=(()=>{class n{xhrFactory;constructor(t){this.xhrFactory=t}handle(t){if(t.method==="JSONP")throw new Te(-2800,!1);let i=this.xhrFactory;return(i.\u0275loadImpl?Er(i.\u0275loadImpl()):sc(null)).pipe(cd(()=>new Pt(s=>{let o=i.build();if(o.open(t.method,t.urlWithParams),t.withCredentials&&(o.withCredentials=!0),t.headers.forEach((v,m)=>o.setRequestHeader(v,m.join(","))),t.headers.has(z_)||o.setRequestHeader(z_,wT),!t.headers.has(H_)){let v=t.detectContentTypeHeader();v!==null&&o.setRequestHeader(H_,v)}if(t.responseType){let v=t.responseType.toLowerCase();o.responseType=v!=="json"?v:"text"}let a=t.serializeBody(),c=null,l=()=>{if(c!==null)return c;let v=o.statusText||"OK",m=new nr(o.getAllResponseHeaders()),p=PT(o)||t.url;return c=new al({headers:m,status:o.status,statusText:v,url:p}),c},u=()=>{let{headers:v,status:m,statusText:p,url:S}=l(),E=null;m!==TT&&(E=typeof o.response>"u"?o.responseText:o.response),m===0&&(m=E?CT:0);let b=m>=200&&m<300;if(t.responseType==="json"&&typeof E=="string"){let F=E;E=E.replace(RT,"");try{E=E!==""?JSON.parse(E):null}catch(T){E=F,b&&(b=!1,E={error:T,text:E})}}b?(s.next(new oa({body:E,headers:v,status:m,statusText:p,url:S||void 0})),s.complete()):s.error(new aa({error:E,headers:v,status:m,statusText:p,url:S||void 0}))},d=v=>{let{url:m}=l(),p=new aa({error:v,status:o.status||0,statusText:o.statusText||"Unknown Error",url:m||void 0});s.error(p)},h=!1,f=v=>{h||(s.next(l()),h=!0);let m={type:Gr.DownloadProgress,loaded:v.loaded};v.lengthComputable&&(m.total=v.total),t.responseType==="text"&&o.responseText&&(m.partialText=o.responseText),s.next(m)},g=v=>{let m={type:Gr.UploadProgress,loaded:v.loaded};v.lengthComputable&&(m.total=v.total),s.next(m)};return o.addEventListener("load",u),o.addEventListener("error",d),o.addEventListener("timeout",d),o.addEventListener("abort",d),t.reportProgress&&(o.addEventListener("progress",f),a!==null&&o.upload&&o.upload.addEventListener("progress",g)),o.send(a),s.next({type:Gr.Sent}),()=>{o.removeEventListener("error",d),o.removeEventListener("abort",d),o.removeEventListener("load",u),o.removeEventListener("timeout",d),t.reportProgress&&(o.removeEventListener("progress",f),a!==null&&o.upload&&o.upload.removeEventListener("progress",g)),o.readyState!==o.DONE&&o.abort()}})))}static \u0275fac=function(i){return new(i||n)(Xe(Hr))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})(),Y_=new Ne(""),OT="XSRF-TOKEN",FT=new Ne("",{providedIn:"root",factory:()=>OT}),LT="X-XSRF-TOKEN",kT=new Ne("",{providedIn:"root",factory:()=>LT}),ca=class{},UT=(()=>{class n{doc;cookieName;lastCookieString="";lastToken=null;parseCount=0;constructor(t,i){this.doc=t,this.cookieName=i}getToken(){let t=this.doc.cookie||"";return t!==this.lastCookieString&&(this.parseCount++,this.lastToken=ea(t,this.cookieName),this.lastCookieString=t),this.lastToken}static \u0275fac=function(i){return new(i||n)(Xe(Fn),Xe(FT))};static \u0275prov=tt({token:n,factory:n.\u0275fac})}return n})(),VT=/^(?:https?:)?\/\//i;function BT(n,e){if(!qe(Y_)||n.method==="GET"||n.method==="HEAD"||VT.test(n.url))return e(n);let t=qe(ca).getToken(),i=qe(kT);return t!=null&&!n.headers.has(i)&&(n=n.clone({headers:n.headers.set(i,t)})),e(n)}function yh(...n){let e=[ll,vh,cl,{provide:Ps,useExisting:cl},{provide:sa,useFactory:()=>qe(DT,{optional:!0})??qe(vh)},{provide:$_,useValue:BT,multi:!0},{provide:Y_,useValue:!0},{provide:ca,useClass:UT}];for(let t of n)e.push(...t.\u0275providers);return xv(e)}var Ut=class n{constructor(){this.speechEnabled=!0;this.isListening=!1;this.speechResult$=new dn;this.audioIntensity$=new dn;this.avatarState$=new dn;this.recognition=null;this.audioCtx=null;this.initAudioContext(),this.initSpeechRecognition()}initAudioContext(){try{let e=window.AudioContext||window.webkitAudioContext;e&&(this.audioCtx=new e)}catch{}}playSciFiTone(e="beep"){if(this.audioCtx)try{this.audioCtx.state==="suspended"&&this.audioCtx.resume();let t=this.audioCtx.createOscillator(),i=this.audioCtx.createGain();t.connect(i),i.connect(this.audioCtx.destination);let r=this.audioCtx.currentTime;e==="boot"?(t.type="sine",t.frequency.setValueAtTime(440,r),t.frequency.exponentialRampToValueAtTime(880,r+.2),i.gain.setValueAtTime(.15,r),i.gain.linearRampToValueAtTime(.01,r+.3),t.start(r),t.stop(r+.3)):e==="ack"?(t.type="sine",t.frequency.setValueAtTime(600,r),t.frequency.setValueAtTime(900,r+.08),i.gain.setValueAtTime(.1,r),i.gain.linearRampToValueAtTime(.01,r+.18),t.start(r),t.stop(r+.18)):e==="error"?(t.type="sawtooth",t.frequency.setValueAtTime(250,r),t.frequency.linearRampToValueAtTime(150,r+.2),i.gain.setValueAtTime(.12,r),i.gain.linearRampToValueAtTime(.01,r+.25),t.start(r),t.stop(r+.25)):(t.type="sine",t.frequency.setValueAtTime(750,r),i.gain.setValueAtTime(.08,r),i.gain.linearRampToValueAtTime(.01,r+.08),t.start(r),t.stop(r+.08))}catch{}}initSpeechRecognition(){let e=window.SpeechRecognition||window.webkitSpeechRecognition;e&&(this.recognition=new e,this.recognition.continuous=!1,this.recognition.interimResults=!1,this.recognition.lang="en-US",this.recognition.onstart=()=>{this.isListening=!0,this.playSciFiTone("ack"),this.avatarState$.next("listening")},this.recognition.onresult=t=>{let i=t.results[0][0].transcript;this.speechResult$.next(i)},this.recognition.onerror=()=>{this.stopListening()},this.recognition.onend=()=>{this.stopListening()})}startListening(){if(this.recognition&&!this.isListening)try{this.recognition.start()}catch{}}stopListening(){if(this.isListening=!1,this.recognition)try{this.recognition.stop()}catch{}this.avatarState$.next("idle")}speak(e){if(!this.speechEnabled||!("speechSynthesis"in window))return;window.speechSynthesis.cancel();let t=e.replace(/```[\s\S]*?```/g,"Code block omitted.").replace(/`([^`]+)`/g,"$1").replace(/[*_#]/g,""),i=new SpeechSynthesisUtterance(t);i.rate=1.02,i.pitch=.94;let s=window.speechSynthesis.getVoices().find(a=>a.name.includes("UK English Male")||a.name.includes("George")||a.name.includes("Oliver")||a.name.includes("Daniel")||a.name.includes("Arthur")||a.lang.includes("en-GB")||a.name.includes("David"));s&&(i.voice=s);let o=null;i.onstart=()=>{this.avatarState$.next("speaking"),o=setInterval(()=>{let a=.25+Math.random()*.75;this.audioIntensity$.next(a)},70)},i.onend=()=>{o&&clearInterval(o),this.audioIntensity$.next(0),this.avatarState$.next("idle")},i.onerror=()=>{o&&clearInterval(o),this.audioIntensity$.next(0),this.avatarState$.next("idle")},window.speechSynthesis.speak(i)}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}};var Qt=class n{constructor(e){this.http=e;this.baseUrl=""}getHealth(){return this.http.get(`${this.baseUrl}/health`)}getTelemetry(){return this.http.get(`${this.baseUrl}/api/system/telemetry`)}getProcesses(e=25){return this.http.get(`${this.baseUrl}/api/system/processes?limit=${e}`)}killProcess(e){return this.http.post(`${this.baseUrl}/api/system/processes/kill`,{pid:e})}setVolume(e,t=null){let i={};return e!==null&&(i.level=e),t!==null&&(i.mute=t),this.http.post(`${this.baseUrl}/api/system/actions/volume`,i)}setBrightness(e){return this.http.post(`${this.baseUrl}/api/system/actions/brightness`,{level:e})}launchApp(e,t=null){return this.http.post(`${this.baseUrl}/api/system/actions/app`,{app_name:e,arguments:t})}powerAction(e){return this.http.post(`${this.baseUrl}/api/system/actions/power`,{mode:e})}takeScreenshot(){return this.http.post(`${this.baseUrl}/api/system/automation/screenshot`,{})}executeTerminal(e,t="powershell"){return this.http.post(`${this.baseUrl}/api/system/terminal/execute`,{command:e,shell_type:t})}getStorageStats(){return this.http.get(`${this.baseUrl}/api/storage/stats`)}listStorageFiles(e=""){return this.http.get(`${this.baseUrl}/api/storage/files?subfolder=${encodeURIComponent(e)}`)}uploadStorageFile(e,t="documents"){let i=new FormData;return i.append("file",e),i.append("subfolder",t),this.http.post(`${this.baseUrl}/api/storage/upload`,i)}indexDirectory(e,t=300){return this.http.post(`${this.baseUrl}/api/storage/index-directory`,{directory_path:e,max_files:t})}searchKnowledge(e){return this.http.post(`${this.baseUrl}/api/storage/search`,{query:e})}chatWithJarvis(e,t="supervisor",i="main_session"){return this.http.post(`${this.baseUrl}/api/agents/chat`,{message:e,target_agent:t,conversation_id:i})}dispatchAgent(e,t,i="main_session"){return this.http.post(`${this.baseUrl}/api/agents/dispatch`,{agent_id:e,instruction:t,conversation_id:i})}getAgentStatus(){return this.http.get(`${this.baseUrl}/api/agents/status`)}static{this.\u0275fac=function(t){return new(t||n)(Xe(ll))}}static{this.\u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}};var ul=class n{constructor(e,t){this.audioService=e;this.apiService=t;this.isConnected=!1;this.netSpeedText="NET: ACTIVE";this.screenshotCaptured=new Ot}toggleVoice(){this.audioService.speechEnabled=!this.audioService.speechEnabled,this.audioService.speechEnabled?this.audioService.playSciFiTone("ack"):"speechSynthesis"in window&&window.speechSynthesis.cancel()}takeScreenshot(){this.audioService.playSciFiTone("ack"),this.apiService.takeScreenshot().subscribe(e=>{e.status==="success"&&this.screenshotCaptured.emit(e.preview_base64)})}lockWorkstation(){confirm("Lock host workstation protocols?")&&(this.audioService.playSciFiTone("ack"),this.apiService.powerAction("lock").subscribe())}static{this.\u0275fac=function(t){return new(t||n)(Se(Ut),Se(Qt))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-hud-header"]],inputs:{isConnected:"isConnected",netSpeedText:"netSpeedText"},outputs:{screenshotCaptured:"screenshotCaptured"},decls:32,vars:6,consts:[[1,"hud-header"],[1,"brand"],[1,"fas","fa-atom","brand-icon"],[1,"brand-title"],[1,"brand-subtitle"],[1,"hud-status-bar"],[1,"status-badge",3,"ngClass"],[1,"indicator"],[1,"status-badge","online"],[1,"fas","fa-database",2,"color","var(--neon-cyan)"],[1,"fas","fa-network-wired",2,"color","var(--neon-green)"],[1,"hud-actions"],[1,"btn-cyber",3,"click"],[1,"fas",3,"ngClass"],[1,"fas","fa-camera"],[1,"btn-cyber","danger",3,"click"],[1,"fas","fa-lock"]],template:function(t,i){t&1&&(V(0,"header",0)(1,"div",1),ve(2,"i",2),V(3,"div")(4,"div",3),ee(5,"J.A.R.V.I.S. AIOS"),G(),V(6,"div",4),ee(7,"LOCAL-FIRST MULTI-AGENT OPERATING SYSTEM"),G()()(),V(8,"div",5)(9,"div",6),ve(10,"div",7),V(11,"span"),ee(12,"LLM: OLLAMA (QWEN3:8B)"),G()(),V(13,"div",8),ve(14,"i",9),V(15,"span"),ee(16,"VAULT: 50GB LOCAL DB"),G()(),V(17,"div",8),ve(18,"i",10),V(19,"span"),ee(20),G()()(),V(21,"div",11)(22,"button",12),Ie("click",function(){return i.toggleVoice()}),ve(23,"i",13),V(24,"span"),ee(25),G()(),V(26,"button",12),Ie("click",function(){return i.takeScreenshot()}),ve(27,"i",14),ee(28," CAPTURE "),G(),V(29,"button",15),Ie("click",function(){return i.lockWorkstation()}),ve(30,"i",16),ee(31," LOCK OS "),G()()()),t&2&&(ne(9),Ye("ngClass",i.isConnected?"online":"busy"),ne(11),Xt(i.netSpeedText),ne(2),_t("active",i.audioService.speechEnabled),ne(),Ye("ngClass",i.audioService.speechEnabled?"fa-volume-high":"fa-volume-xmark"),ne(2),Ct("VOICE: ",i.audioService.speechEnabled?"ON":"OFF",""))},dependencies:[Rt,Ti],styles:[".hud-header[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;background:var(--bg-panel);border:1px solid var(--border-cyan);border-radius:8px;padding:8px 16px;backdrop-filter:blur(14px);box-shadow:0 0 16px #00f0ff26;flex-wrap:wrap;gap:8px}.brand[_ngcontent-%COMP%]{display:flex;align-items:center;gap:12px}.brand-icon[_ngcontent-%COMP%]{font-size:24px;color:var(--neon-cyan);animation:_ngcontent-%COMP%_pulse-glow 2s infinite ease-in-out}.brand-title[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:18px;letter-spacing:3px;color:#fff;text-shadow:0 0 8px var(--neon-cyan)}.brand-subtitle[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:11px;color:var(--neon-cyan);letter-spacing:1px}.hud-status-bar[_ngcontent-%COMP%]{display:flex;align-items:center;gap:12px;font-family:var(--font-data);font-size:13px;flex-wrap:wrap}.status-badge[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px;padding:4px 10px;background:#00f0ff14;border:1px solid rgba(0,240,255,.3);border-radius:4px;color:var(--text-main);font-size:12px}.status-badge.online[_ngcontent-%COMP%]   .indicator[_ngcontent-%COMP%]{width:8px;height:8px;border-radius:50%;background:var(--neon-green);box-shadow:0 0 8px var(--neon-green)}.status-badge.busy[_ngcontent-%COMP%]   .indicator[_ngcontent-%COMP%]{width:8px;height:8px;border-radius:50%;background:var(--neon-gold);box-shadow:0 0 8px var(--neon-gold)}.hud-actions[_ngcontent-%COMP%]{display:flex;gap:8px;align-items:center}.btn-cyber[_ngcontent-%COMP%]{background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);font-family:var(--font-hud);font-size:11px;letter-spacing:1px;padding:6px 12px;border-radius:4px;cursor:pointer;transition:all .2s ease;display:inline-flex;align-items:center;gap:6px}.btn-cyber[_ngcontent-%COMP%]:hover{background:var(--neon-cyan);color:#000;box-shadow:0 0 12px var(--neon-cyan)}.btn-cyber.active[_ngcontent-%COMP%]{background:var(--neon-cyan);color:#000;box-shadow:0 0 10px var(--neon-cyan)}.btn-cyber.danger[_ngcontent-%COMP%]{border-color:var(--neon-red);color:var(--neon-red);background:#ff336626}.btn-cyber.danger[_ngcontent-%COMP%]:hover{background:var(--neon-red);color:#fff;box-shadow:0 0 12px var(--neon-red)}@keyframes _ngcontent-%COMP%_pulse-glow{0%,to{opacity:.8;filter:drop-shadow(0 0 4px var(--neon-cyan))}50%{opacity:1;filter:drop-shadow(0 0 12px var(--neon-cyan))}}"]})}};var zT=0,Z_=1,GT=2;var ix=1,WT=2,ki=3,fr=0,_n=1,Vi=2,ur=0,Js=1,J_=2,K_=3,Q_=4,jT=5,Kr=100,$T=101,qT=102,XT=103,YT=104,ZT=200,JT=201,KT=202,QT=203,Jh=204,Kh=205,eD=206,tD=207,nD=208,iD=209,rD=210,sD=211,oD=212,aD=213,cD=214,Qh=0,ep=1,tp=2,to=3,np=4,ip=5,rp=6,sp=7,rx=0,lD=1,uD=2,dr=0,dD=1,fD=2,hD=3,pD=4,mD=5,gD=6,vD=7;var e0=300,no=301,io=302,op=303,ap=304,uu=306,cp=1e3,ts=1001,lp=1002,Zn=1003,yD=1004;var dl=1005;var ai=1006,_h=1007;var ns=1008;var Gi=1009,sx=1010,ox=1011,ya=1012,hm=1013,is=1014,Bi=1015,Ma=1016,pm=1017,mm=1018,ro=1020,ax=35902,cx=1021,lx=1022,Yn=1023,ux=1024,dx=1025,Ks=1026,so=1027,fx=1028,gm=1029,hx=1030,vm=1031;var ym=1033,Fl=33776,Ll=33777,kl=33778,Ul=33779,up=35840,dp=35841,fp=35842,hp=35843,pp=36196,mp=37492,gp=37496,vp=37808,yp=37809,_p=37810,xp=37811,bp=37812,Mp=37813,Ep=37814,Sp=37815,wp=37816,Cp=37817,Tp=37818,Dp=37819,Ap=37820,Ip=37821,Vl=36492,Rp=36494,Np=36495,px=36283,Pp=36284,Op=36285,Fp=36286;var Bl=2300,Lp=2301,xh=2302,t0=2400,n0=2401,i0=2402;var _D=3200,xD=3201;var bD=0,MD=1,lr="",Un="srgb",fo="srgb-linear",du="linear",ht="srgb";var Fs=7680;var r0=519,ED=512,SD=513,wD=514,mx=515,CD=516,TD=517,DD=518,AD=519,s0=35044;var o0="300 es",Hi=2e3,Hl=2001,hr=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){if(this._listeners===void 0)return!1;let i=this._listeners;return i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){if(this._listeners===void 0)return;let r=this._listeners[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){if(this._listeners===void 0)return;let i=this._listeners[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},en=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var bh=Math.PI/180,kp=180/Math.PI;function Ea(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(en[n&255]+en[n>>8&255]+en[n>>16&255]+en[n>>24&255]+"-"+en[e&255]+en[e>>8&255]+"-"+en[e>>16&15|64]+en[e>>24&255]+"-"+en[t&63|128]+en[t>>8&255]+"-"+en[t>>16&255]+en[t>>24&255]+en[i&255]+en[i>>8&255]+en[i>>16&255]+en[i>>24&255]).toLowerCase()}function yn(n,e,t){return Math.max(e,Math.min(t,n))}function ID(n,e){return(n%e+e)%e}function Mh(n,e,t){return(1-t)*n+t*e}function la(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function vn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var st=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(yn(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Ve=class n{constructor(e,t,i,r,s,o,a,c,l){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],h=i[2],f=i[5],g=i[8],v=r[0],m=r[3],p=r[6],S=r[1],E=r[4],b=r[7],F=r[2],T=r[5],C=r[8];return s[0]=o*v+a*S+c*F,s[3]=o*m+a*E+c*T,s[6]=o*p+a*b+c*C,s[1]=l*v+u*S+d*F,s[4]=l*m+u*E+d*T,s[7]=l*p+u*b+d*C,s[2]=h*v+f*S+g*F,s[5]=h*m+f*E+g*T,s[8]=h*p+f*b+g*C,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,h=a*c-u*s,f=l*s-o*c,g=t*d+i*h+r*f;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let v=1/g;return e[0]=d*v,e[1]=(r*l-u*i)*v,e[2]=(a*i-r*o)*v,e[3]=h*v,e[4]=(u*t-r*c)*v,e[5]=(r*s-a*t)*v,e[6]=f*v,e[7]=(i*c-l*t)*v,e[8]=(o*t-i*s)*v,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Eh.makeScale(e,t)),this}rotate(e){return this.premultiply(Eh.makeRotation(-e)),this}translate(e,t){return this.premultiply(Eh.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Eh=new Ve;function gx(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function zl(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function RD(){let n=zl("canvas");return n.style.display="block",n}var a0={};function ma(n){n in a0||(a0[n]=!0,console.warn(n))}function ND(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}function PD(n){let e=n.elements;e[2]=.5*e[2]+.5*e[3],e[6]=.5*e[6]+.5*e[7],e[10]=.5*e[10]+.5*e[11],e[14]=.5*e[14]+.5*e[15]}function OD(n){let e=n.elements;e[11]===-1?(e[10]=-e[10]-1,e[14]=-e[14]):(e[10]=-e[10],e[14]=-e[14]+1)}var rt={enabled:!0,workingColorSpace:fo,spaces:{},convert:function(n,e,t){return this.enabled===!1||e===t||!e||!t||(this.spaces[e].transfer===ht&&(n.r=zi(n.r),n.g=zi(n.g),n.b=zi(n.b)),this.spaces[e].primaries!==this.spaces[t].primaries&&(n.applyMatrix3(this.spaces[e].toXYZ),n.applyMatrix3(this.spaces[t].fromXYZ)),this.spaces[t].transfer===ht&&(n.r=Qs(n.r),n.g=Qs(n.g),n.b=Qs(n.b))),n},fromWorkingColorSpace:function(n,e){return this.convert(n,this.workingColorSpace,e)},toWorkingColorSpace:function(n,e){return this.convert(n,e,this.workingColorSpace)},getPrimaries:function(n){return this.spaces[n].primaries},getTransfer:function(n){return n===lr?du:this.spaces[n].transfer},getLuminanceCoefficients:function(n,e=this.workingColorSpace){return n.fromArray(this.spaces[e].luminanceCoefficients)},define:function(n){Object.assign(this.spaces,n)},_getMatrix:function(n,e,t){return n.copy(this.spaces[e].toXYZ).multiply(this.spaces[t].fromXYZ)},_getDrawingBufferColorSpace:function(n){return this.spaces[n].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(n=this.workingColorSpace){return this.spaces[n].workingColorSpaceConfig.unpackColorSpace}};function zi(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function Qs(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var c0=[.64,.33,.3,.6,.15,.06],l0=[.2126,.7152,.0722],u0=[.3127,.329],d0=new Ve().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),f0=new Ve().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);rt.define({[fo]:{primaries:c0,whitePoint:u0,transfer:du,toXYZ:d0,fromXYZ:f0,luminanceCoefficients:l0,workingColorSpaceConfig:{unpackColorSpace:Un},outputColorSpaceConfig:{drawingBufferColorSpace:Un}},[Un]:{primaries:c0,whitePoint:u0,transfer:ht,toXYZ:d0,fromXYZ:f0,luminanceCoefficients:l0,outputColorSpaceConfig:{drawingBufferColorSpace:Un}}});var Ls,Up=class{static getDataURL(e){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let t;if(e instanceof HTMLCanvasElement)t=e;else{Ls===void 0&&(Ls=zl("canvas")),Ls.width=e.width,Ls.height=e.height;let i=Ls.getContext("2d");e instanceof ImageData?i.putImageData(e,0,0):i.drawImage(e,0,0,e.width,e.height),t=Ls}return t.width>2048||t.height>2048?(console.warn("THREE.ImageUtils.getDataURL: Image converted to jpg for performance reasons",e),t.toDataURL("image/jpeg",.6)):t.toDataURL("image/png")}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=zl("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=zi(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(zi(t[i]/255)*255):t[i]=zi(t[i]);return{data:t,width:e.width,height:e.height}}else return console.warn("THREE.ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},FD=0,Gl=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:FD++}),this.uuid=Ea(),this.data=e,this.dataReady=!0,this.version=0}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Sh(r[o].image)):s.push(Sh(r[o]))}else s=Sh(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Sh(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Up.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(console.warn("THREE.Texture: Unable to serialize Texture."),{})}var LD=0,cs=(()=>{class n extends hr{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=ts,s=ts,o=ai,a=ns,c=Yn,l=Gi,u=n.DEFAULT_ANISOTROPY,d=lr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:LD++}),this.uuid=Ea(),this.name="",this.source=new Gl(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new st(0,0),this.repeat=new st(1,1),this.center=new st(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Ve,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.version=0,this.onUpdate=null,this.isRenderTargetTexture=!1,this.pmremVersion=0}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.6,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==e0)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case cp:t.x=t.x-Math.floor(t.x);break;case ts:t.x=t.x<0?0:1;break;case lp:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case cp:t.y=t.y-Math.floor(t.y);break;case ts:t.y=t.y<0?0:1;break;case lp:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=e0,n.DEFAULT_ANISOTROPY=1,n})(),pt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],h=c[1],f=c[5],g=c[9],v=c[2],m=c[6],p=c[10];if(Math.abs(u-h)<.01&&Math.abs(d-v)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+h)<.1&&Math.abs(d+v)<.1&&Math.abs(g+m)<.1&&Math.abs(l+f+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let E=(l+1)/2,b=(f+1)/2,F=(p+1)/2,T=(u+h)/4,C=(d+v)/4,I=(g+m)/4;return E>b&&E>F?E<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(E),r=T/i,s=C/i):b>F?b<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(b),i=T/r,s=I/r):F<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(F),i=C/s,r=I/s),this.set(i,r,s,t),this}let S=Math.sqrt((m-g)*(m-g)+(d-v)*(d-v)+(h-u)*(h-u));return Math.abs(S)<.001&&(S=1),this.x=(m-g)/S,this.y=(d-v)/S,this.z=(h-u)/S,this.w=Math.acos((l+f+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this.w=Math.max(e.w,Math.min(t.w,this.w)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this.w=Math.max(e,Math.min(t,this.w)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Vp=class extends hr{constructor(e=1,t=1,i={}){super(),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=1,this.scissor=new pt(0,0,e,t),this.scissorTest=!1,this.viewport=new pt(0,0,e,t);let r={width:e,height:t,depth:1};i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:ai,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1},i);let s=new cs(r,i.mapping,i.wrapS,i.wrapT,i.magFilter,i.minFilter,i.format,i.type,i.anisotropy,i.colorSpace);s.flipY=!1,s.generateMipmaps=i.generateMipmaps,s.internalFormat=i.internalFormat,this.textures=[];let o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0;this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.depthTexture=i.depthTexture,this.samples=i.samples}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i;this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let i=0,r=e.textures.length;i<r;i++)this.textures[i]=e.textures[i].clone(),this.textures[i].isRenderTargetTexture=!0;let t=Object.assign({},e.texture.image);return this.texture.source=new Gl(t),this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},Wi=class extends Vp{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Wl=class extends cs{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Zn,this.minFilter=Zn,this.wrapR=ts,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Bp=class extends cs{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Zn,this.minFilter=Zn,this.wrapR=ts,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var pr=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],h=s[o+0],f=s[o+1],g=s[o+2],v=s[o+3];if(a===0){e[t+0]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d;return}if(a===1){e[t+0]=h,e[t+1]=f,e[t+2]=g,e[t+3]=v;return}if(d!==v||c!==h||l!==f||u!==g){let m=1-a,p=c*h+l*f+u*g+d*v,S=p>=0?1:-1,E=1-p*p;if(E>Number.EPSILON){let F=Math.sqrt(E),T=Math.atan2(F,p*S);m=Math.sin(m*T)/F,a=Math.sin(a*T)/F}let b=a*S;if(c=c*m+h*b,l=l*m+f*b,u=u*m+g*b,d=d*m+v*b,m===1-a){let F=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=F,l*=F,u*=F,d*=F}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],h=s[o+1],f=s[o+2],g=s[o+3];return e[t]=a*g+u*d+c*f-l*h,e[t+1]=c*g+u*h+l*d-a*f,e[t+2]=l*g+u*f+a*h-c*d,e[t+3]=u*g-a*d-c*h-l*f,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),h=c(i/2),f=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=h*u*d+l*f*g,this._y=l*f*d-h*u*g,this._z=l*u*g+h*f*d,this._w=l*u*d-h*f*g;break;case"YXZ":this._x=h*u*d+l*f*g,this._y=l*f*d-h*u*g,this._z=l*u*g-h*f*d,this._w=l*u*d+h*f*g;break;case"ZXY":this._x=h*u*d-l*f*g,this._y=l*f*d+h*u*g,this._z=l*u*g+h*f*d,this._w=l*u*d-h*f*g;break;case"ZYX":this._x=h*u*d-l*f*g,this._y=l*f*d+h*u*g,this._z=l*u*g-h*f*d,this._w=l*u*d+h*f*g;break;case"YZX":this._x=h*u*d+l*f*g,this._y=l*f*d+h*u*g,this._z=l*u*g-h*f*d,this._w=l*u*d-h*f*g;break;case"XZY":this._x=h*u*d-l*f*g,this._y=l*f*d-h*u*g,this._z=l*u*g+h*f*d,this._w=l*u*d+h*f*g;break;default:console.warn("THREE.Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],h=i+a+d;if(h>0){let f=.5/Math.sqrt(h+1);this._w=.25/f,this._x=(u-c)*f,this._y=(s-l)*f,this._z=(o-r)*f}else if(i>a&&i>d){let f=2*Math.sqrt(1+i-a-d);this._w=(u-c)/f,this._x=.25*f,this._y=(r+o)/f,this._z=(s+l)/f}else if(a>d){let f=2*Math.sqrt(1+a-i-d);this._w=(s-l)/f,this._x=(r+o)/f,this._y=.25*f,this._z=(c+u)/f}else{let f=2*Math.sqrt(1+d-i-a);this._w=(o-r)/f,this._x=(s+l)/f,this._y=(c+u)/f,this._z=.25*f}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<Number.EPSILON?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(yn(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){if(t===0)return this;if(t===1)return this.copy(e);let i=this._x,r=this._y,s=this._z,o=this._w,a=o*e._w+i*e._x+r*e._y+s*e._z;if(a<0?(this._w=-e._w,this._x=-e._x,this._y=-e._y,this._z=-e._z,a=-a):this.copy(e),a>=1)return this._w=o,this._x=i,this._y=r,this._z=s,this;let c=1-a*a;if(c<=Number.EPSILON){let f=1-t;return this._w=f*o+t*this._w,this._x=f*i+t*this._x,this._y=f*r+t*this._y,this._z=f*s+t*this._z,this.normalize(),this}let l=Math.sqrt(c),u=Math.atan2(l,a),d=Math.sin((1-t)*u)/l,h=Math.sin(t*u)/l;return this._w=o*d+this._w*h,this._x=i*d+this._x*h,this._y=r*d+this._y*h,this._z=s*d+this._z*h,this._onChangeCallback(),this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},O=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(h0.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(h0.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return wh.copy(this).projectOnVector(e),this.sub(wh)}reflect(e){return this.sub(wh.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(yn(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},wh=new O,h0=new pr,rs=class{constructor(e=new O(1/0,1/0,1/0),t=new O(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint($n.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint($n.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=$n.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,$n):$n.fromBufferAttribute(s,o),$n.applyMatrix4(e.matrixWorld),this.expandByPoint($n);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),fl.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),fl.copy(i.boundingBox)),fl.applyMatrix4(e.matrixWorld),this.union(fl)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,$n),$n.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(ua),hl.subVectors(this.max,ua),ks.subVectors(e.a,ua),Us.subVectors(e.b,ua),Vs.subVectors(e.c,ua),ir.subVectors(Us,ks),rr.subVectors(Vs,Us),jr.subVectors(ks,Vs);let t=[0,-ir.z,ir.y,0,-rr.z,rr.y,0,-jr.z,jr.y,ir.z,0,-ir.x,rr.z,0,-rr.x,jr.z,0,-jr.x,-ir.y,ir.x,0,-rr.y,rr.x,0,-jr.y,jr.x,0];return!Ch(t,ks,Us,Vs,hl)||(t=[1,0,0,0,1,0,0,0,1],!Ch(t,ks,Us,Vs,hl))?!1:(pl.crossVectors(ir,rr),t=[pl.x,pl.y,pl.z],Ch(t,ks,Us,Vs,hl))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,$n).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize($n).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Ni[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Ni[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Ni[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Ni[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Ni[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Ni[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Ni[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Ni[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Ni),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}},Ni=[new O,new O,new O,new O,new O,new O,new O,new O],$n=new O,fl=new rs,ks=new O,Us=new O,Vs=new O,ir=new O,rr=new O,jr=new O,ua=new O,hl=new O,pl=new O,$r=new O;function Ch(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){$r.fromArray(n,s);let a=r.x*Math.abs($r.x)+r.y*Math.abs($r.y)+r.z*Math.abs($r.z),c=e.dot($r),l=t.dot($r),u=i.dot($r);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var kD=new rs,da=new O,Th=new O,oo=class{constructor(e=new O,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):kD.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;da.subVectors(e,this.center);let t=da.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(da,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Th.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(da.copy(e.center).add(Th)),this.expandByPoint(da.copy(e.center).sub(Th))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}},Pi=new O,Dh=new O,ml=new O,sr=new O,Ah=new O,gl=new O,Ih=new O,jl=class{constructor(e=new O,t=new O(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Pi)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Pi.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Pi.copy(this.origin).addScaledVector(this.direction,t),Pi.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Dh.copy(e).add(t).multiplyScalar(.5),ml.copy(t).sub(e).normalize(),sr.copy(this.origin).sub(Dh);let s=e.distanceTo(t)*.5,o=-this.direction.dot(ml),a=sr.dot(this.direction),c=-sr.dot(ml),l=sr.lengthSq(),u=Math.abs(1-o*o),d,h,f,g;if(u>0)if(d=o*c-a,h=o*a-c,g=s*u,d>=0)if(h>=-g)if(h<=g){let v=1/u;d*=v,h*=v,f=d*(d+o*h+2*a)+h*(o*d+h+2*c)+l}else h=s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*c)+l;else h=-s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*c)+l;else h<=-g?(d=Math.max(0,-(-o*s+a)),h=d>0?-s:Math.min(Math.max(-s,-c),s),f=-d*d+h*(h+2*c)+l):h<=g?(d=0,h=Math.min(Math.max(-s,-c),s),f=h*(h+2*c)+l):(d=Math.max(0,-(o*s+a)),h=d>0?s:Math.min(Math.max(-s,-c),s),f=-d*d+h*(h+2*c)+l);else h=o>0?-s:s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Dh).addScaledVector(ml,h),f}intersectSphere(e,t){Pi.subVectors(e.center,this.origin);let i=Pi.dot(this.direction),r=Pi.dot(Pi)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,h=this.origin;return l>=0?(i=(e.min.x-h.x)*l,r=(e.max.x-h.x)*l):(i=(e.max.x-h.x)*l,r=(e.min.x-h.x)*l),u>=0?(s=(e.min.y-h.y)*u,o=(e.max.y-h.y)*u):(s=(e.max.y-h.y)*u,o=(e.min.y-h.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-h.z)*d,c=(e.max.z-h.z)*d):(a=(e.max.z-h.z)*d,c=(e.min.z-h.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Pi)!==null}intersectTriangle(e,t,i,r,s){Ah.subVectors(t,e),gl.subVectors(i,e),Ih.crossVectors(Ah,gl);let o=this.direction.dot(Ih),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;sr.subVectors(this.origin,e);let c=a*this.direction.dot(gl.crossVectors(sr,gl));if(c<0)return null;let l=a*this.direction.dot(Ah.cross(sr));if(l<0||c+l>o)return null;let u=-a*sr.dot(Ih);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Tt=class n{constructor(e,t,i,r,s,o,a,c,l,u,d,h,f,g,v,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,h,f,g,v,m)}set(e,t,i,r,s,o,a,c,l,u,d,h,f,g,v,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=h,p[3]=f,p[7]=g,p[11]=v,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){let t=this.elements,i=e.elements,r=1/Bs.setFromMatrixColumn(e,0).length(),s=1/Bs.setFromMatrixColumn(e,1).length(),o=1/Bs.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let h=o*u,f=o*d,g=a*u,v=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=f+g*l,t[5]=h-v*l,t[9]=-a*c,t[2]=v-h*l,t[6]=g+f*l,t[10]=o*c}else if(e.order==="YXZ"){let h=c*u,f=c*d,g=l*u,v=l*d;t[0]=h+v*a,t[4]=g*a-f,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=f*a-g,t[6]=v+h*a,t[10]=o*c}else if(e.order==="ZXY"){let h=c*u,f=c*d,g=l*u,v=l*d;t[0]=h-v*a,t[4]=-o*d,t[8]=g+f*a,t[1]=f+g*a,t[5]=o*u,t[9]=v-h*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let h=o*u,f=o*d,g=a*u,v=a*d;t[0]=c*u,t[4]=g*l-f,t[8]=h*l+v,t[1]=c*d,t[5]=v*l+h,t[9]=f*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let h=o*c,f=o*l,g=a*c,v=a*l;t[0]=c*u,t[4]=v-h*d,t[8]=g*d+f,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=f*d+g,t[10]=h-v*d}else if(e.order==="XZY"){let h=o*c,f=o*l,g=a*c,v=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=h*d+v,t[5]=o*u,t[9]=f*d-g,t[2]=g*d-f,t[6]=a*u,t[10]=v*d+h}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(UD,e,VD)}lookAt(e,t,i){let r=this.elements;return En.subVectors(e,t),En.lengthSq()===0&&(En.z=1),En.normalize(),or.crossVectors(i,En),or.lengthSq()===0&&(Math.abs(i.z)===1?En.x+=1e-4:En.z+=1e-4,En.normalize(),or.crossVectors(i,En)),or.normalize(),vl.crossVectors(En,or),r[0]=or.x,r[4]=vl.x,r[8]=En.x,r[1]=or.y,r[5]=vl.y,r[9]=En.y,r[2]=or.z,r[6]=vl.z,r[10]=En.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],h=i[9],f=i[13],g=i[2],v=i[6],m=i[10],p=i[14],S=i[3],E=i[7],b=i[11],F=i[15],T=r[0],C=r[4],I=r[8],M=r[12],x=r[1],D=r[5],W=r[9],B=r[13],Z=r[2],J=r[6],q=r[10],Q=r[14],z=r[3],ae=r[7],he=r[11],we=r[15];return s[0]=o*T+a*x+c*Z+l*z,s[4]=o*C+a*D+c*J+l*ae,s[8]=o*I+a*W+c*q+l*he,s[12]=o*M+a*B+c*Q+l*we,s[1]=u*T+d*x+h*Z+f*z,s[5]=u*C+d*D+h*J+f*ae,s[9]=u*I+d*W+h*q+f*he,s[13]=u*M+d*B+h*Q+f*we,s[2]=g*T+v*x+m*Z+p*z,s[6]=g*C+v*D+m*J+p*ae,s[10]=g*I+v*W+m*q+p*he,s[14]=g*M+v*B+m*Q+p*we,s[3]=S*T+E*x+b*Z+F*z,s[7]=S*C+E*D+b*J+F*ae,s[11]=S*I+E*W+b*q+F*he,s[15]=S*M+E*B+b*Q+F*we,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],h=e[10],f=e[14],g=e[3],v=e[7],m=e[11],p=e[15];return g*(+s*c*d-r*l*d-s*a*h+i*l*h+r*a*f-i*c*f)+v*(+t*c*f-t*l*h+s*o*h-r*o*f+r*l*u-s*c*u)+m*(+t*l*d-t*a*f-s*o*d+i*o*f+s*a*u-i*l*u)+p*(-r*a*u-t*c*d+t*a*h+r*o*d-i*o*h+i*c*u)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],h=e[10],f=e[11],g=e[12],v=e[13],m=e[14],p=e[15],S=d*m*l-v*h*l+v*c*f-a*m*f-d*c*p+a*h*p,E=g*h*l-u*m*l-g*c*f+o*m*f+u*c*p-o*h*p,b=u*v*l-g*d*l+g*a*f-o*v*f-u*a*p+o*d*p,F=g*d*c-u*v*c-g*a*h+o*v*h+u*a*m-o*d*m,T=t*S+i*E+r*b+s*F;if(T===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let C=1/T;return e[0]=S*C,e[1]=(v*h*s-d*m*s-v*r*f+i*m*f+d*r*p-i*h*p)*C,e[2]=(a*m*s-v*c*s+v*r*l-i*m*l-a*r*p+i*c*p)*C,e[3]=(d*c*s-a*h*s-d*r*l+i*h*l+a*r*f-i*c*f)*C,e[4]=E*C,e[5]=(u*m*s-g*h*s+g*r*f-t*m*f-u*r*p+t*h*p)*C,e[6]=(g*c*s-o*m*s-g*r*l+t*m*l+o*r*p-t*c*p)*C,e[7]=(o*h*s-u*c*s+u*r*l-t*h*l-o*r*f+t*c*f)*C,e[8]=b*C,e[9]=(g*d*s-u*v*s-g*i*f+t*v*f+u*i*p-t*d*p)*C,e[10]=(o*v*s-g*a*s+g*i*l-t*v*l-o*i*p+t*a*p)*C,e[11]=(u*a*s-o*d*s-u*i*l+t*d*l+o*i*f-t*a*f)*C,e[12]=F*C,e[13]=(u*v*r-g*d*r+g*i*h-t*v*h-u*i*m+t*d*m)*C,e[14]=(g*a*r-o*v*r-g*i*c+t*v*c+o*i*m-t*a*m)*C,e[15]=(o*d*r-u*a*r+u*i*c-t*d*c-o*i*h+t*a*h)*C,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,h=s*l,f=s*u,g=s*d,v=o*u,m=o*d,p=a*d,S=c*l,E=c*u,b=c*d,F=i.x,T=i.y,C=i.z;return r[0]=(1-(v+p))*F,r[1]=(f+b)*F,r[2]=(g-E)*F,r[3]=0,r[4]=(f-b)*T,r[5]=(1-(h+p))*T,r[6]=(m+S)*T,r[7]=0,r[8]=(g+E)*C,r[9]=(m-S)*C,r[10]=(1-(h+v))*C,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements,s=Bs.set(r[0],r[1],r[2]).length(),o=Bs.set(r[4],r[5],r[6]).length(),a=Bs.set(r[8],r[9],r[10]).length();this.determinant()<0&&(s=-s),e.x=r[12],e.y=r[13],e.z=r[14],qn.copy(this);let l=1/s,u=1/o,d=1/a;return qn.elements[0]*=l,qn.elements[1]*=l,qn.elements[2]*=l,qn.elements[4]*=u,qn.elements[5]*=u,qn.elements[6]*=u,qn.elements[8]*=d,qn.elements[9]*=d,qn.elements[10]*=d,t.setFromRotationMatrix(qn),i.x=s,i.y=o,i.z=a,this}makePerspective(e,t,i,r,s,o,a=Hi){let c=this.elements,l=2*s/(t-e),u=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),f,g;if(a===Hi)f=-(o+s)/(o-s),g=-2*o*s/(o-s);else if(a===Hl)f=-o/(o-s),g=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return c[0]=l,c[4]=0,c[8]=d,c[12]=0,c[1]=0,c[5]=u,c[9]=h,c[13]=0,c[2]=0,c[6]=0,c[10]=f,c[14]=g,c[3]=0,c[7]=0,c[11]=-1,c[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=Hi){let c=this.elements,l=1/(t-e),u=1/(i-r),d=1/(o-s),h=(t+e)*l,f=(i+r)*u,g,v;if(a===Hi)g=(o+s)*d,v=-2*d;else if(a===Hl)g=s*d,v=-1*d;else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return c[0]=2*l,c[4]=0,c[8]=0,c[12]=-h,c[1]=0,c[5]=2*u,c[9]=0,c[13]=-f,c[2]=0,c[6]=0,c[10]=v,c[14]=-g,c[3]=0,c[7]=0,c[11]=0,c[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Bs=new O,qn=new Tt,UD=new O(0,0,0),VD=new O(1,1,1),or=new O,vl=new O,En=new O,p0=new Tt,m0=new pr,ao=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],d=s[9],h=s[2],f=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(yn(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(f,u),this._z=0);break;case"YXZ":this._x=Math.asin(-yn(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-h,o),this._z=0);break;case"ZXY":this._x=Math.asin(yn(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(-h,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-yn(h,-1,1)),Math.abs(h)<.9999999?(this._x=Math.atan2(f,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(yn(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-h,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-yn(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(f,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:console.warn("THREE.Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return p0.makeRotationFromQuaternion(t),this.setFromRotationMatrix(p0,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return m0.setFromEuler(this),this.setFromQuaternion(m0,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),$l=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},BD=0,g0=new O,Hs=new pr,Oi=new Tt,yl=new O,fa=new O,HD=new O,zD=new pr,v0=new O(1,0,0),y0=new O(0,1,0),_0=new O(0,0,1),x0={type:"added"},GD={type:"removed"},zs={type:"childadded",child:null},Rh={type:"childremoved",child:null},mr=(()=>{class n extends hr{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:BD++}),this.uuid=Ea(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new O,i=new ao,r=new pr,s=new O(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Tt},normalMatrix:{value:new Ve}}),this.matrix=new Tt,this.matrixWorld=new Tt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new $l,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.userData={}}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return Hs.setFromAxisAngle(t,i),this.quaternion.multiply(Hs),this}rotateOnWorldAxis(t,i){return Hs.setFromAxisAngle(t,i),this.quaternion.premultiply(Hs),this}rotateX(t){return this.rotateOnAxis(v0,t)}rotateY(t){return this.rotateOnAxis(y0,t)}rotateZ(t){return this.rotateOnAxis(_0,t)}translateOnAxis(t,i){return g0.copy(t).applyQuaternion(this.quaternion),this.position.add(g0.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(v0,t)}translateY(t){return this.translateOnAxis(y0,t)}translateZ(t){return this.translateOnAxis(_0,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Oi.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?yl.copy(t):yl.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),fa.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Oi.lookAt(fa,yl,this.up):Oi.lookAt(yl,fa,this.up),this.quaternion.setFromRotationMatrix(Oi),s&&(Oi.extractRotation(s.matrixWorld),Hs.setFromRotationMatrix(Oi),this.quaternion.premultiply(Hs.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(console.error("THREE.Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(x0),zs.child=t,this.dispatchEvent(zs),zs.child=null):console.error("THREE.Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(GD),Rh.child=t,this.dispatchEvent(Rh),Rh.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Oi.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Oi.multiply(t.parent.matrixWorld)),t.applyMatrix4(Oi),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(x0),zs.child=t,this.dispatchEvent(zs),zs.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(fa,t,HD),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(fa,zD,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale),this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.6,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.visibility=this._visibility,s.active=this._active,s.bounds=this._bounds.map(c=>({boxInitialized:c.boxInitialized,boxMin:c.box.min.toArray(),boxMax:c.box.max.toArray(),sphereInitialized:c.sphereInitialized,sphereRadius:c.sphere.radius,sphereCenter:c.sphere.center.toArray()})),s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.geometryCount=this._geometryCount,s.matricesTexture=this._matricesTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere={center:s.boundingSphere.center.toArray(),radius:s.boundingSphere.radius}),this.boundingBox!==null&&(s.boundingBox={min:s.boundingBox.min.toArray(),max:s.boundingBox.max.toArray()}));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let h=l[u];o(t.shapes,h)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),h=a(t.shapes),f=a(t.skeletons),g=a(t.animations),v=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),h.length>0&&(r.shapes=h),f.length>0&&(r.skeletons=f),g.length>0&&(r.animations=g),v.length>0&&(r.nodes=v)}return r.object=s,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new O(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Xn=new O,Fi=new O,Nh=new O,Li=new O,Gs=new O,Ws=new O,b0=new O,Ph=new O,Oh=new O,Fh=new O,Lh=new pt,kh=new pt,Uh=new pt,Qr=class n{constructor(e=new O,t=new O,i=new O){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Xn.subVectors(e,t),r.cross(Xn);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Xn.subVectors(r,t),Fi.subVectors(i,t),Nh.subVectors(e,t);let o=Xn.dot(Xn),a=Xn.dot(Fi),c=Xn.dot(Nh),l=Fi.dot(Fi),u=Fi.dot(Nh),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let h=1/d,f=(l*c-a*u)*h,g=(o*u-a*c)*h;return s.set(1-f-g,g,f)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Li)===null?!1:Li.x>=0&&Li.y>=0&&Li.x+Li.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,Li)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,Li.x),c.addScaledVector(o,Li.y),c.addScaledVector(a,Li.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return Lh.setScalar(0),kh.setScalar(0),Uh.setScalar(0),Lh.fromBufferAttribute(e,t),kh.fromBufferAttribute(e,i),Uh.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Lh,s.x),o.addScaledVector(kh,s.y),o.addScaledVector(Uh,s.z),o}static isFrontFacing(e,t,i,r){return Xn.subVectors(i,t),Fi.subVectors(e,t),Xn.cross(Fi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Xn.subVectors(this.c,this.b),Fi.subVectors(this.a,this.b),Xn.cross(Fi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;Gs.subVectors(r,i),Ws.subVectors(s,i),Ph.subVectors(e,i);let c=Gs.dot(Ph),l=Ws.dot(Ph);if(c<=0&&l<=0)return t.copy(i);Oh.subVectors(e,r);let u=Gs.dot(Oh),d=Ws.dot(Oh);if(u>=0&&d<=u)return t.copy(r);let h=c*d-u*l;if(h<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(Gs,o);Fh.subVectors(e,s);let f=Gs.dot(Fh),g=Ws.dot(Fh);if(g>=0&&f<=g)return t.copy(s);let v=f*l-c*g;if(v<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(Ws,a);let m=u*g-f*d;if(m<=0&&d-u>=0&&f-g>=0)return b0.subVectors(s,r),a=(d-u)/(d-u+(f-g)),t.copy(r).addScaledVector(b0,a);let p=1/(m+v+h);return o=v*p,a=h*p,t.copy(i).addScaledVector(Gs,o).addScaledVector(Ws,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},vx={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},ar={h:0,s:0,l:0},_l={h:0,s:0,l:0};function Vh(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var at=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Un){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,rt.toWorkingColorSpace(this,t),this}setRGB(e,t,i,r=rt.workingColorSpace){return this.r=e,this.g=t,this.b=i,rt.toWorkingColorSpace(this,r),this}setHSL(e,t,i,r=rt.workingColorSpace){if(e=ID(e,1),t=yn(t,0,1),i=yn(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Vh(o,s,e+1/3),this.g=Vh(o,s,e),this.b=Vh(o,s,e-1/3)}return rt.toWorkingColorSpace(this,r),this}setStyle(e,t=Un){function i(s){s!==void 0&&parseFloat(s)<1&&console.warn("THREE.Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:console.warn("THREE.Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);console.warn("THREE.Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Un){let i=vx[e.toLowerCase()];return i!==void 0?this.setHex(i,t):console.warn("THREE.Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=zi(e.r),this.g=zi(e.g),this.b=zi(e.b),this}copyLinearToSRGB(e){return this.r=Qs(e.r),this.g=Qs(e.g),this.b=Qs(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Un){return rt.fromWorkingColorSpace(tn.copy(this),e),Math.round(yn(tn.r*255,0,255))*65536+Math.round(yn(tn.g*255,0,255))*256+Math.round(yn(tn.b*255,0,255))}getHexString(e=Un){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=rt.workingColorSpace){rt.fromWorkingColorSpace(tn.copy(this),t);let i=tn.r,r=tn.g,s=tn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=rt.workingColorSpace){return rt.fromWorkingColorSpace(tn.copy(this),t),e.r=tn.r,e.g=tn.g,e.b=tn.b,e}getStyle(e=Un){rt.fromWorkingColorSpace(tn.copy(this),e);let t=tn.r,i=tn.g,r=tn.b;return e!==Un?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(ar),this.setHSL(ar.h+e,ar.s+t,ar.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(ar),e.getHSL(_l);let i=Mh(ar.h,_l.h,t),r=Mh(ar.s,_l.s,t),s=Mh(ar.l,_l.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},tn=new at;at.NAMES=vx;var WD=0,ss=class extends hr{static get type(){return"Material"}get type(){return this.constructor.type}set type(e){}constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:WD++}),this.uuid=Ea(),this.name="",this.blending=Js,this.side=fr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Jh,this.blendDst=Kh,this.blendEquation=Kr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new at(0,0,0),this.blendAlpha=0,this.depthFunc=to,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=r0,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Fs,this.stencilZFail=Fs,this.stencilZPass=Fs,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){console.warn(`THREE.Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){console.warn(`THREE.Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.6,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Js&&(i.blending=this.blending),this.side!==fr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Jh&&(i.blendSrc=this.blendSrc),this.blendDst!==Kh&&(i.blendDst=this.blendDst),this.blendEquation!==Kr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==to&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==r0&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Fs&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Fs&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Fs&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}onBuild(){console.warn("Material: onBuild() has been removed.")}},ci=class extends ss{static get type(){return"MeshBasicMaterial"}constructor(e){super(),this.isMeshBasicMaterial=!0,this.color=new at(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new ao,this.combine=rx,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};var Vt=new O,xl=new st,xn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=s0,this.updateRanges=[],this.gpuType=Bi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)xl.fromBufferAttribute(this,t),xl.applyMatrix3(e),this.setXY(t,xl.x,xl.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Vt.fromBufferAttribute(this,t),Vt.applyMatrix3(e),this.setXYZ(t,Vt.x,Vt.y,Vt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Vt.fromBufferAttribute(this,t),Vt.applyMatrix4(e),this.setXYZ(t,Vt.x,Vt.y,Vt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Vt.fromBufferAttribute(this,t),Vt.applyNormalMatrix(e),this.setXYZ(t,Vt.x,Vt.y,Vt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Vt.fromBufferAttribute(this,t),Vt.transformDirection(e),this.setXYZ(t,Vt.x,Vt.y,Vt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=la(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=vn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=la(t,this.array)),t}setX(e,t){return this.normalized&&(t=vn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=la(t,this.array)),t}setY(e,t){return this.normalized&&(t=vn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=la(t,this.array)),t}setZ(e,t){return this.normalized&&(t=vn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=la(t,this.array)),t}setW(e,t){return this.normalized&&(t=vn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=vn(t,this.array),i=vn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=vn(t,this.array),i=vn(i,this.array),r=vn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=vn(t,this.array),i=vn(i,this.array),r=vn(r,this.array),s=vn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==s0&&(e.usage=this.usage),e}};var ql=class extends xn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Xl=class extends xn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var $t=class extends xn{constructor(e,t,i){super(new Float32Array(e),t,i)}},jD=0,kn=new Tt,Bh=new mr,js=new O,Sn=new rs,ha=new rs,jt=new O,wn=class n extends hr{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:jD++}),this.uuid=Ea(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(gx(e)?Xl:ql)(e,1):this.index=e,this}setIndirect(e){return this.indirect=e,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Ve().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return kn.makeRotationFromQuaternion(e),this.applyMatrix4(kn),this}rotateX(e){return kn.makeRotationX(e),this.applyMatrix4(kn),this}rotateY(e){return kn.makeRotationY(e),this.applyMatrix4(kn),this}rotateZ(e){return kn.makeRotationZ(e),this.applyMatrix4(kn),this}translate(e,t,i){return kn.makeTranslation(e,t,i),this.applyMatrix4(kn),this}scale(e,t,i){return kn.makeScale(e,t,i),this.applyMatrix4(kn),this}lookAt(e){return Bh.lookAt(e),Bh.updateMatrix(),this.applyMatrix4(Bh.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(js).negate(),this.translate(js.x,js.y,js.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new $t(i,3))}else{for(let i=0,r=t.count;i<r;i++){let s=e[i];t.setXYZ(i,s.x,s.y,s.z||0)}e.length>t.count&&console.warn("THREE.BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new rs);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new O(-1/0,-1/0,-1/0),new O(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Sn.setFromBufferAttribute(s),this.morphTargetsRelative?(jt.addVectors(this.boundingBox.min,Sn.min),this.boundingBox.expandByPoint(jt),jt.addVectors(this.boundingBox.max,Sn.max),this.boundingBox.expandByPoint(jt)):(this.boundingBox.expandByPoint(Sn.min),this.boundingBox.expandByPoint(Sn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&console.error('THREE.BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new oo);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new O,1/0);return}if(e){let i=this.boundingSphere.center;if(Sn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];ha.setFromBufferAttribute(a),this.morphTargetsRelative?(jt.addVectors(Sn.min,ha.min),Sn.expandByPoint(jt),jt.addVectors(Sn.max,ha.max),Sn.expandByPoint(jt)):(Sn.expandByPoint(ha.min),Sn.expandByPoint(ha.max))}Sn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)jt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(jt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)jt.fromBufferAttribute(a,l),c&&(js.fromBufferAttribute(e,l),jt.add(js)),r=Math.max(r,i.distanceToSquared(jt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error('THREE.BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){console.error("THREE.BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new xn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let I=0;I<i.count;I++)a[I]=new O,c[I]=new O;let l=new O,u=new O,d=new O,h=new st,f=new st,g=new st,v=new O,m=new O;function p(I,M,x){l.fromBufferAttribute(i,I),u.fromBufferAttribute(i,M),d.fromBufferAttribute(i,x),h.fromBufferAttribute(s,I),f.fromBufferAttribute(s,M),g.fromBufferAttribute(s,x),u.sub(l),d.sub(l),f.sub(h),g.sub(h);let D=1/(f.x*g.y-g.x*f.y);isFinite(D)&&(v.copy(u).multiplyScalar(g.y).addScaledVector(d,-f.y).multiplyScalar(D),m.copy(d).multiplyScalar(f.x).addScaledVector(u,-g.x).multiplyScalar(D),a[I].add(v),a[M].add(v),a[x].add(v),c[I].add(m),c[M].add(m),c[x].add(m))}let S=this.groups;S.length===0&&(S=[{start:0,count:e.count}]);for(let I=0,M=S.length;I<M;++I){let x=S[I],D=x.start,W=x.count;for(let B=D,Z=D+W;B<Z;B+=3)p(e.getX(B+0),e.getX(B+1),e.getX(B+2))}let E=new O,b=new O,F=new O,T=new O;function C(I){F.fromBufferAttribute(r,I),T.copy(F);let M=a[I];E.copy(M),E.sub(F.multiplyScalar(F.dot(M))).normalize(),b.crossVectors(T,M);let D=b.dot(c[I])<0?-1:1;o.setXYZW(I,E.x,E.y,E.z,D)}for(let I=0,M=S.length;I<M;++I){let x=S[I],D=x.start,W=x.count;for(let B=D,Z=D+W;B<Z;B+=3)C(e.getX(B+0)),C(e.getX(B+1)),C(e.getX(B+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new xn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let h=0,f=i.count;h<f;h++)i.setXYZ(h,0,0,0);let r=new O,s=new O,o=new O,a=new O,c=new O,l=new O,u=new O,d=new O;if(e)for(let h=0,f=e.count;h<f;h+=3){let g=e.getX(h+0),v=e.getX(h+1),m=e.getX(h+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,v),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,v),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(v,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let h=0,f=t.count;h<f;h+=3)r.fromBufferAttribute(t,h+0),s.fromBufferAttribute(t,h+1),o.fromBufferAttribute(t,h+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(h+0,u.x,u.y,u.z),i.setXYZ(h+1,u.x,u.y,u.z),i.setXYZ(h+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)jt.fromBufferAttribute(e,t),jt.normalize(),e.setXYZ(t,jt.x,jt.y,jt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,h=new l.constructor(c.length*u),f=0,g=0;for(let v=0,m=c.length;v<m;v++){a.isInterleavedBufferAttribute?f=c[v]*a.data.stride+a.offset:f=c[v]*u;for(let p=0;p<u;p++)h[g++]=l[f++]}return new xn(h,u,d)}if(this.index===null)return console.warn("THREE.BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let h=l[u],f=e(h,i);c.push(f)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.6,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,h=l.length;d<h;d++){let f=l[d];u.push(f.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere={center:a.center.toArray(),radius:a.radius}),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone(t));let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let h=0,f=d.length;h<f;h++)u.push(d[h].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},M0=new Tt,qr=new jl,bl=new oo,E0=new O,Ml=new O,El=new O,Sl=new O,Hh=new O,wl=new O,S0=new O,Cl=new O,Yt=class extends mr{constructor(e=new wn,t=new ci){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){wl.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(Hh.fromBufferAttribute(d,e),o?wl.addScaledVector(Hh,u):wl.addScaledVector(Hh.sub(t),u))}t.add(wl)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),bl.copy(i.boundingSphere),bl.applyMatrix4(s),qr.copy(e.ray).recast(e.near),!(bl.containsPoint(qr.origin)===!1&&(qr.intersectSphere(bl,E0)===null||qr.origin.distanceToSquared(E0)>(e.far-e.near)**2))&&(M0.copy(s).invert(),qr.copy(e.ray).applyMatrix4(M0),!(i.boundingBox!==null&&qr.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,qr)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,h=s.groups,f=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,v=h.length;g<v;g++){let m=h[g],p=o[m.materialIndex],S=Math.max(m.start,f.start),E=Math.min(a.count,Math.min(m.start+m.count,f.start+f.count));for(let b=S,F=E;b<F;b+=3){let T=a.getX(b),C=a.getX(b+1),I=a.getX(b+2);r=Tl(this,p,e,i,l,u,d,T,C,I),r&&(r.faceIndex=Math.floor(b/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),v=Math.min(a.count,f.start+f.count);for(let m=g,p=v;m<p;m+=3){let S=a.getX(m),E=a.getX(m+1),b=a.getX(m+2);r=Tl(this,o,e,i,l,u,d,S,E,b),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,v=h.length;g<v;g++){let m=h[g],p=o[m.materialIndex],S=Math.max(m.start,f.start),E=Math.min(c.count,Math.min(m.start+m.count,f.start+f.count));for(let b=S,F=E;b<F;b+=3){let T=b,C=b+1,I=b+2;r=Tl(this,p,e,i,l,u,d,T,C,I),r&&(r.faceIndex=Math.floor(b/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),v=Math.min(c.count,f.start+f.count);for(let m=g,p=v;m<p;m+=3){let S=m,E=m+1,b=m+2;r=Tl(this,o,e,i,l,u,d,S,E,b),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function $D(n,e,t,i,r,s,o,a){let c;if(e.side===_n?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===fr,a),c===null)return null;Cl.copy(a),Cl.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(Cl);return l<t.near||l>t.far?null:{distance:l,point:Cl.clone(),object:n}}function Tl(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,Ml),n.getVertexPosition(c,El),n.getVertexPosition(l,Sl);let u=$D(n,e,t,i,Ml,El,Sl,S0);if(u){let d=new O;Qr.getBarycoord(S0,Ml,El,Sl,d),r&&(u.uv=Qr.getInterpolatedAttribute(r,a,c,l,d,new st)),s&&(u.uv1=Qr.getInterpolatedAttribute(s,a,c,l,d,new st)),o&&(u.normal=Qr.getInterpolatedAttribute(o,a,c,l,d,new O),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let h={a,b:c,c:l,normal:new O,materialIndex:0};Qr.getNormal(Ml,El,Sl,h.normal),u.face=h,u.barycoord=d}return u}var _a=class n extends wn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],h=0,f=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new $t(l,3)),this.setAttribute("normal",new $t(u,3)),this.setAttribute("uv",new $t(d,2));function g(v,m,p,S,E,b,F,T,C,I,M){let x=b/C,D=F/I,W=b/2,B=F/2,Z=T/2,J=C+1,q=I+1,Q=0,z=0,ae=new O;for(let he=0;he<q;he++){let we=he*D-B;for(let je=0;je<J;je++){let mt=je*x-W;ae[v]=mt*S,ae[m]=we*E,ae[p]=Z,l.push(ae.x,ae.y,ae.z),ae[v]=0,ae[m]=0,ae[p]=T>0?1:-1,u.push(ae.x,ae.y,ae.z),d.push(je/C),d.push(1-he/I),Q+=1}}for(let he=0;he<I;he++)for(let we=0;we<C;we++){let je=h+we+J*he,mt=h+we+J*(he+1),$=h+(we+1)+J*(he+1),re=h+(we+1)+J*he;c.push(je,mt,re),c.push(mt,$,re),z+=6}a.addGroup(f,z,M),f+=z,h+=Q}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};function co(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(console.warn("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function an(n){let e={};for(let t=0;t<n.length;t++){let i=co(n[t]);for(let r in i)e[r]=i[r]}return e}function qD(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function yx(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:rt.workingColorSpace}var XD={clone:co,merge:an},YD=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,ZD=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,li=class extends ss{static get type(){return"ShaderMaterial"}constructor(e){super(),this.isShaderMaterial=!0,this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=YD,this.fragmentShader=ZD,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=co(e.uniforms),this.uniformsGroups=qD(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Yl=class extends mr{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Tt,this.projectionMatrix=new Tt,this.projectionMatrixInverse=new Tt,this.coordinateSystem=Hi}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorldInverse.copy(this.matrixWorld).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorldInverse.copy(this.matrixWorld).invert()}clone(){return new this.constructor().copy(this)}},cr=new O,w0=new st,C0=new st,nn=class extends Yl{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=kp*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(bh*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return kp*2*Math.atan(Math.tan(bh*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){cr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(cr.x,cr.y).multiplyScalar(-e/cr.z),cr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(cr.x,cr.y).multiplyScalar(-e/cr.z)}getViewSize(e,t){return this.getViewBounds(e,w0,C0),t.subVectors(C0,w0)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(bh*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}},$s=-90,qs=1,Hp=class extends mr{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new nn($s,qs,e,t);r.layers=this.layers,this.add(r);let s=new nn($s,qs,e,t);s.layers=this.layers,this.add(s);let o=new nn($s,qs,e,t);o.layers=this.layers,this.add(o);let a=new nn($s,qs,e,t);a.layers=this.layers,this.add(a);let c=new nn($s,qs,e,t);c.layers=this.layers,this.add(c);let l=new nn($s,qs,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===Hi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Hl)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),h=e.getActiveCubeFace(),f=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let v=i.texture.generateMipmaps;i.texture.generateMipmaps=!1,e.setRenderTarget(i,0,r),e.render(t,s),e.setRenderTarget(i,1,r),e.render(t,o),e.setRenderTarget(i,2,r),e.render(t,a),e.setRenderTarget(i,3,r),e.render(t,c),e.setRenderTarget(i,4,r),e.render(t,l),i.texture.generateMipmaps=v,e.setRenderTarget(i,5,r),e.render(t,u),e.setRenderTarget(d,h,f),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},Zl=class extends cs{constructor(e,t,i,r,s,o,a,c,l,u){e=e!==void 0?e:[],t=t!==void 0?t:no,super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},zp=class extends Wi{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Zl(r,t.mapping,t.wrapS,t.wrapT,t.magFilter,t.minFilter,t.format,t.type,t.anisotropy,t.colorSpace),this.texture.isRenderTargetTexture=!0,this.texture.generateMipmaps=t.generateMipmaps!==void 0?t.generateMipmaps:!1,this.texture.minFilter=t.minFilter!==void 0?t.minFilter:ai}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new _a(5,5,5),s=new li({name:"CubemapFromEquirect",uniforms:co(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:_n,blending:ur});s.uniforms.tEquirect.value=t;let o=new Yt(r,s),a=t.minFilter;return t.minFilter===ns&&(t.minFilter=ai),new Hp(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t,i,r){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},zh=new O,JD=new O,KD=new Ve,Ui=class{constructor(e=new O(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=zh.subVectors(i,t).cross(JD.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(zh),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||KD.getNormalMatrix(e),r=this.coplanarPoint(zh).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Xr=new oo,Dl=new O,xa=class{constructor(e=new Ui,t=new Ui,i=new Ui,r=new Ui,s=new Ui,o=new Ui){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Hi){let i=this.planes,r=e.elements,s=r[0],o=r[1],a=r[2],c=r[3],l=r[4],u=r[5],d=r[6],h=r[7],f=r[8],g=r[9],v=r[10],m=r[11],p=r[12],S=r[13],E=r[14],b=r[15];if(i[0].setComponents(c-s,h-l,m-f,b-p).normalize(),i[1].setComponents(c+s,h+l,m+f,b+p).normalize(),i[2].setComponents(c+o,h+u,m+g,b+S).normalize(),i[3].setComponents(c-o,h-u,m-g,b-S).normalize(),i[4].setComponents(c-a,h-d,m-v,b-E).normalize(),t===Hi)i[5].setComponents(c+a,h+d,m+v,b+E).normalize();else if(t===Hl)i[5].setComponents(a,d,v,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Xr.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Xr.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Xr)}intersectsSprite(e){return Xr.center.set(0,0,0),Xr.radius=.7071067811865476,Xr.applyMatrix4(e.matrixWorld),this.intersectsSphere(Xr)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Dl.x=r.normal.x>0?e.max.x:e.min.x,Dl.y=r.normal.y>0?e.max.y:e.min.y,Dl.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Dl)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};function _x(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function QD(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,h=n.createBuffer();n.bindBuffer(c,h),n.bufferData(c,l,u),a.onUploadCallback();let f;if(l instanceof Float32Array)f=n.FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?f=n.HALF_FLOAT:f=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)f=n.SHORT;else if(l instanceof Uint32Array)f=n.UNSIGNED_INT;else if(l instanceof Int32Array)f=n.INT;else if(l instanceof Int8Array)f=n.BYTE;else if(l instanceof Uint8Array)f=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)f=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:h,type:f,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((f,g)=>f.start-g.start);let h=0;for(let f=1;f<d.length;f++){let g=d[h],v=d[f];v.start<=g.start+g.count+1?g.count=Math.max(g.count,v.start+v.count-g.start):(++h,d[h]=v)}d.length=h+1;for(let f=0,g=d.length;f<g;f++){let v=d[f];n.bufferSubData(l,v.start*u.BYTES_PER_ELEMENT,u,v.start,v.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var Jl=class n extends wn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,h=t/c,f=[],g=[],v=[],m=[];for(let p=0;p<u;p++){let S=p*h-o;for(let E=0;E<l;E++){let b=E*d-s;g.push(b,-S,0),v.push(0,0,1),m.push(E/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let S=0;S<a;S++){let E=S+l*p,b=S+l*(p+1),F=S+1+l*(p+1),T=S+1+l*p;f.push(E,b,T),f.push(b,F,T)}this.setIndex(f),this.setAttribute("position",new $t(g,3)),this.setAttribute("normal",new $t(v,3)),this.setAttribute("uv",new $t(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}},eA=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,tA=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,nA=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,iA=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,rA=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,sA=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,oA=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,aA=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,cA=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec3 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 ).rgb;
	}
#endif`,lA=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,uA=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,dA=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,fA=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,hA=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,pA=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,mA=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,gA=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,vA=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,yA=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,_A=`#if defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#elif defined( USE_COLOR )
	diffuseColor.rgb *= vColor;
#endif`,xA=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR )
	varying vec3 vColor;
#endif`,bA=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec3 vColor;
#endif`,MA=`#if defined( USE_COLOR_ALPHA )
	vColor = vec4( 1.0 );
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec3( 1.0 );
#endif
#ifdef USE_COLOR
	vColor *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.xyz *= instanceColor.xyz;
#endif
#ifdef USE_BATCHING_COLOR
	vec3 batchingColor = getBatchingColor( getIndirectIndex( gl_DrawID ) );
	vColor.xyz *= batchingColor.xyz;
#endif`,EA=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
mat3 transposeMat3( const in mat3 m ) {
	mat3 tmp;
	tmp[ 0 ] = vec3( m[ 0 ].x, m[ 1 ].x, m[ 2 ].x );
	tmp[ 1 ] = vec3( m[ 0 ].y, m[ 1 ].y, m[ 2 ].y );
	tmp[ 2 ] = vec3( m[ 0 ].z, m[ 1 ].z, m[ 2 ].z );
	return tmp;
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,SA=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,wA=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,CA=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,TA=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,DA=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,AA=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,IA="gl_FragColor = linearToOutputTexel( gl_FragColor );",RA=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,NA=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
	#else
		vec4 envColor = vec4( 0.0 );
	#endif
	#ifdef ENVMAP_BLENDING_MULTIPLY
		outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_MIX )
		outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_ADD )
		outgoingLight += envColor.xyz * specularStrength * reflectivity;
	#endif
#endif`,PA=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
	
#endif`,OA=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,FA=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,LA=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,kA=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,UA=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,VA=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,BA=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,HA=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,zA=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,GA=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,WA=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,jA=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,$A=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, roughness * roughness) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,qA=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,XA=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,YA=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,ZA=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,JA=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb * ( 1.0 - metalnessFactor );
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = mix( min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = mix( vec3( 0.04 ), diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.07, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,KA=`struct PhysicalMaterial {
	vec3 diffuseColor;
	float roughness;
	vec3 specularColor;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return saturate(v);
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColor;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transposeMat3( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float a = roughness < 0.25 ? -339.2 * r2 + 161.4 * roughness - 25.9 : -8.48 * r2 + 14.3 * roughness - 9.95;
	float b = roughness < 0.25 ? 44.0 * r2 - 23.7 * roughness + 3.26 : 1.97 * r2 - 3.27 * roughness + 0.72;
	float DG = exp( a * dotNV + b ) + ( roughness < 0.25 ? 0.0 : 0.1 * ( roughness - 0.25 ) );
	return saturate( DG * RECIPROCAL_PI );
}
vec2 DFGApprox( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	const vec4 c0 = vec4( - 1, - 0.0275, - 0.572, 0.022 );
	const vec4 c1 = vec4( 1, 0.0425, 1.04, - 0.04 );
	vec4 r = roughness * c0 + c1;
	float a004 = min( r.x * r.x, exp2( - 9.28 * dotNV ) ) * r.x + r.y;
	vec2 fab = vec2( - 1.04, 1.04 ) * a004 + r.zw;
	return fab;
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColor * t2.x + ( vec3( 1.0 ) - material.specularColor ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseColor * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
	#endif
	vec3 singleScattering = vec3( 0.0 );
	vec3 multiScattering = vec3( 0.0 );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnel, material.roughness, singleScattering, multiScattering );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScattering, multiScattering );
	#endif
	vec3 totalScattering = singleScattering + multiScattering;
	vec3 diffuse = material.diffuseColor * ( 1.0 - max( max( totalScattering.r, totalScattering.g ), totalScattering.b ) );
	reflectedLight.indirectSpecular += radiance * singleScattering;
	reflectedLight.indirectSpecular += multiScattering * cosineWeightedIrradiance;
	reflectedLight.indirectDiffuse += diffuse * cosineWeightedIrradiance;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,QA=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnel = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,eI=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD ) && defined( ENVMAP_TYPE_CUBE_UV )
		iblIrradiance += getIBLIrradiance( geometryNormal );
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,tI=`#if defined( RE_IndirectDiffuse )
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,nI=`#if defined( USE_LOGDEPTHBUF )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,iI=`#if defined( USE_LOGDEPTHBUF )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,rI=`#ifdef USE_LOGDEPTHBUF
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,sI=`#ifdef USE_LOGDEPTHBUF
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,oI=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,aI=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,cI=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,lI=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,uI=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,dI=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,fI=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,hI=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,pI=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,mI=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,gI=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,vI=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,yI=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,_I=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,xI=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,bI=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,MI=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,EI=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,SI=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,wI=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,CI=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,TI=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,DI=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return depth * ( near - far ) - near;
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return ( near * far ) / ( ( far - near ) * depth - far );
}`,AI=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,II=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,RI=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,NI=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,PI=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,OI=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,FI=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform sampler2D pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	float texture2DCompare( sampler2D depths, vec2 uv, float compare ) {
		return step( compare, unpackRGBAToDepth( texture2D( depths, uv ) ) );
	}
	vec2 texture2DDistribution( sampler2D shadow, vec2 uv ) {
		return unpackRGBATo2Half( texture2D( shadow, uv ) );
	}
	float VSMShadow (sampler2D shadow, vec2 uv, float compare ){
		float occlusion = 1.0;
		vec2 distribution = texture2DDistribution( shadow, uv );
		float hard_shadow = step( compare , distribution.x );
		if (hard_shadow != 1.0 ) {
			float distance = compare - distribution.x ;
			float variance = max( 0.00000, distribution.y * distribution.y );
			float softness_probability = variance / (variance + distance * distance );			softness_probability = clamp( ( softness_probability - 0.3 ) / ( 0.95 - 0.3 ), 0.0, 1.0 );			occlusion = clamp( max( hard_shadow, softness_probability ), 0.0, 1.0 );
		}
		return occlusion;
	}
	float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
		float shadow = 1.0;
		shadowCoord.xyz /= shadowCoord.w;
		shadowCoord.z += shadowBias;
		bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
		bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
		if ( frustumTest ) {
		#if defined( SHADOWMAP_TYPE_PCF )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx0 = - texelSize.x * shadowRadius;
			float dy0 = - texelSize.y * shadowRadius;
			float dx1 = + texelSize.x * shadowRadius;
			float dy1 = + texelSize.y * shadowRadius;
			float dx2 = dx0 / 2.0;
			float dy2 = dy0 / 2.0;
			float dx3 = dx1 / 2.0;
			float dy3 = dy1 / 2.0;
			shadow = (
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy1 ), shadowCoord.z )
			) * ( 1.0 / 17.0 );
		#elif defined( SHADOWMAP_TYPE_PCF_SOFT )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx = texelSize.x;
			float dy = texelSize.y;
			vec2 uv = shadowCoord.xy;
			vec2 f = fract( uv * shadowMapSize + 0.5 );
			uv -= f * texelSize;
			shadow = (
				texture2DCompare( shadowMap, uv, shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( dx, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( 0.0, dy ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + texelSize, shadowCoord.z ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, 0.0 ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 0.0 ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, dy ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( 0.0, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 0.0, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( texture2DCompare( shadowMap, uv + vec2( dx, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( dx, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( mix( texture2DCompare( shadowMap, uv + vec2( -dx, -dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, -dy ), shadowCoord.z ),
						  f.x ),
					 mix( texture2DCompare( shadowMap, uv + vec2( -dx, 2.0 * dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 2.0 * dy ), shadowCoord.z ),
						  f.x ),
					 f.y )
			) * ( 1.0 / 9.0 );
		#elif defined( SHADOWMAP_TYPE_VSM )
			shadow = VSMShadow( shadowMap, shadowCoord.xy, shadowCoord.z );
		#else
			shadow = texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z );
		#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	vec2 cubeToUV( vec3 v, float texelSizeY ) {
		vec3 absV = abs( v );
		float scaleToCube = 1.0 / max( absV.x, max( absV.y, absV.z ) );
		absV *= scaleToCube;
		v *= scaleToCube * ( 1.0 - 2.0 * texelSizeY );
		vec2 planar = v.xy;
		float almostATexel = 1.5 * texelSizeY;
		float almostOne = 1.0 - almostATexel;
		if ( absV.z >= almostOne ) {
			if ( v.z > 0.0 )
				planar.x = 4.0 - v.x;
		} else if ( absV.x >= almostOne ) {
			float signX = sign( v.x );
			planar.x = v.z * signX + 2.0 * signX;
		} else if ( absV.y >= almostOne ) {
			float signY = sign( v.y );
			planar.x = v.x + 2.0 * signY + 2.0;
			planar.y = v.z * signY - 2.0;
		}
		return vec2( 0.125, 0.25 ) * planar + vec2( 0.375, 0.75 );
	}
	float getPointShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		
		float lightToPositionLength = length( lightToPosition );
		if ( lightToPositionLength - shadowCameraFar <= 0.0 && lightToPositionLength - shadowCameraNear >= 0.0 ) {
			float dp = ( lightToPositionLength - shadowCameraNear ) / ( shadowCameraFar - shadowCameraNear );			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			vec2 texelSize = vec2( 1.0 ) / ( shadowMapSize * vec2( 4.0, 2.0 ) );
			#if defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_PCF_SOFT ) || defined( SHADOWMAP_TYPE_VSM )
				vec2 offset = vec2( - 1, 1 ) * shadowRadius * texelSize.y;
				shadow = (
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxx, texelSize.y ), dp )
				) * ( 1.0 / 9.0 );
			#else
				shadow = texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp );
			#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
#endif`,LI=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,kI=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,UI=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,VI=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,BI=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,HI=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,zI=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,GI=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,WI=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,jI=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,$I=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,qI=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseColor, material.specularColor, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,XI=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
		
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
		
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		
		#else
		
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,YI=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,ZI=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,JI=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,KI=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,QI=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,e1=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,t1=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,n1=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,i1=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,r1=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,s1=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,o1=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	float fragCoordZ = 0.5 * vHighPrecisionZW[0] / vHighPrecisionZW[1] + 0.5;
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,a1=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,c1=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = packDepthToRGBA( dist );
}`,l1=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,u1=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,d1=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,f1=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,h1=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,p1=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,m1=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,g1=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,v1=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,y1=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,_1=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,x1=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <packing>
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( packNormalToRGB( normal ), diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,b1=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,M1=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,E1=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,S1=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
		float sheenEnergyComp = 1.0 - 0.157 * max3( material.sheenColor );
		outgoingLight = outgoingLight * sheenEnergyComp + sheenSpecularDirect + sheenSpecularIndirect;
	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,w1=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,C1=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,T1=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,D1=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,A1=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,I1=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <packing>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,R1=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,N1=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,ze={alphahash_fragment:eA,alphahash_pars_fragment:tA,alphamap_fragment:nA,alphamap_pars_fragment:iA,alphatest_fragment:rA,alphatest_pars_fragment:sA,aomap_fragment:oA,aomap_pars_fragment:aA,batching_pars_vertex:cA,batching_vertex:lA,begin_vertex:uA,beginnormal_vertex:dA,bsdfs:fA,iridescence_fragment:hA,bumpmap_pars_fragment:pA,clipping_planes_fragment:mA,clipping_planes_pars_fragment:gA,clipping_planes_pars_vertex:vA,clipping_planes_vertex:yA,color_fragment:_A,color_pars_fragment:xA,color_pars_vertex:bA,color_vertex:MA,common:EA,cube_uv_reflection_fragment:SA,defaultnormal_vertex:wA,displacementmap_pars_vertex:CA,displacementmap_vertex:TA,emissivemap_fragment:DA,emissivemap_pars_fragment:AA,colorspace_fragment:IA,colorspace_pars_fragment:RA,envmap_fragment:NA,envmap_common_pars_fragment:PA,envmap_pars_fragment:OA,envmap_pars_vertex:FA,envmap_physical_pars_fragment:$A,envmap_vertex:LA,fog_vertex:kA,fog_pars_vertex:UA,fog_fragment:VA,fog_pars_fragment:BA,gradientmap_pars_fragment:HA,lightmap_pars_fragment:zA,lights_lambert_fragment:GA,lights_lambert_pars_fragment:WA,lights_pars_begin:jA,lights_toon_fragment:qA,lights_toon_pars_fragment:XA,lights_phong_fragment:YA,lights_phong_pars_fragment:ZA,lights_physical_fragment:JA,lights_physical_pars_fragment:KA,lights_fragment_begin:QA,lights_fragment_maps:eI,lights_fragment_end:tI,logdepthbuf_fragment:nI,logdepthbuf_pars_fragment:iI,logdepthbuf_pars_vertex:rI,logdepthbuf_vertex:sI,map_fragment:oI,map_pars_fragment:aI,map_particle_fragment:cI,map_particle_pars_fragment:lI,metalnessmap_fragment:uI,metalnessmap_pars_fragment:dI,morphinstance_vertex:fI,morphcolor_vertex:hI,morphnormal_vertex:pI,morphtarget_pars_vertex:mI,morphtarget_vertex:gI,normal_fragment_begin:vI,normal_fragment_maps:yI,normal_pars_fragment:_I,normal_pars_vertex:xI,normal_vertex:bI,normalmap_pars_fragment:MI,clearcoat_normal_fragment_begin:EI,clearcoat_normal_fragment_maps:SI,clearcoat_pars_fragment:wI,iridescence_pars_fragment:CI,opaque_fragment:TI,packing:DI,premultiplied_alpha_fragment:AI,project_vertex:II,dithering_fragment:RI,dithering_pars_fragment:NI,roughnessmap_fragment:PI,roughnessmap_pars_fragment:OI,shadowmap_pars_fragment:FI,shadowmap_pars_vertex:LI,shadowmap_vertex:kI,shadowmask_pars_fragment:UI,skinbase_vertex:VI,skinning_pars_vertex:BI,skinning_vertex:HI,skinnormal_vertex:zI,specularmap_fragment:GI,specularmap_pars_fragment:WI,tonemapping_fragment:jI,tonemapping_pars_fragment:$I,transmission_fragment:qI,transmission_pars_fragment:XI,uv_pars_fragment:YI,uv_pars_vertex:ZI,uv_vertex:JI,worldpos_vertex:KI,background_vert:QI,background_frag:e1,backgroundCube_vert:t1,backgroundCube_frag:n1,cube_vert:i1,cube_frag:r1,depth_vert:s1,depth_frag:o1,distanceRGBA_vert:a1,distanceRGBA_frag:c1,equirect_vert:l1,equirect_frag:u1,linedashed_vert:d1,linedashed_frag:f1,meshbasic_vert:h1,meshbasic_frag:p1,meshlambert_vert:m1,meshlambert_frag:g1,meshmatcap_vert:v1,meshmatcap_frag:y1,meshnormal_vert:_1,meshnormal_frag:x1,meshphong_vert:b1,meshphong_frag:M1,meshphysical_vert:E1,meshphysical_frag:S1,meshtoon_vert:w1,meshtoon_frag:C1,points_vert:T1,points_frag:D1,shadow_vert:A1,shadow_frag:I1,sprite_vert:R1,sprite_frag:N1},se={common:{diffuse:{value:new at(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Ve},alphaMap:{value:null},alphaMapTransform:{value:new Ve},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Ve}},envmap:{envMap:{value:null},envMapRotation:{value:new Ve},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Ve}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Ve}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Ve},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Ve},normalScale:{value:new st(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Ve},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Ve}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Ve}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Ve}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new at(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMap:{value:[]},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotShadowMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMap:{value:[]},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new at(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Ve},alphaTest:{value:0},uvTransform:{value:new Ve}},sprite:{diffuse:{value:new at(16777215)},opacity:{value:1},center:{value:new st(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Ve},alphaMap:{value:null},alphaMapTransform:{value:new Ve},alphaTest:{value:0}}},oi={basic:{uniforms:an([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.fog]),vertexShader:ze.meshbasic_vert,fragmentShader:ze.meshbasic_frag},lambert:{uniforms:an([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new at(0)}}]),vertexShader:ze.meshlambert_vert,fragmentShader:ze.meshlambert_frag},phong:{uniforms:an([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new at(0)},specular:{value:new at(1118481)},shininess:{value:30}}]),vertexShader:ze.meshphong_vert,fragmentShader:ze.meshphong_frag},standard:{uniforms:an([se.common,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.roughnessmap,se.metalnessmap,se.fog,se.lights,{emissive:{value:new at(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:ze.meshphysical_vert,fragmentShader:ze.meshphysical_frag},toon:{uniforms:an([se.common,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.gradientmap,se.fog,se.lights,{emissive:{value:new at(0)}}]),vertexShader:ze.meshtoon_vert,fragmentShader:ze.meshtoon_frag},matcap:{uniforms:an([se.common,se.bumpmap,se.normalmap,se.displacementmap,se.fog,{matcap:{value:null}}]),vertexShader:ze.meshmatcap_vert,fragmentShader:ze.meshmatcap_frag},points:{uniforms:an([se.points,se.fog]),vertexShader:ze.points_vert,fragmentShader:ze.points_frag},dashed:{uniforms:an([se.common,se.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:ze.linedashed_vert,fragmentShader:ze.linedashed_frag},depth:{uniforms:an([se.common,se.displacementmap]),vertexShader:ze.depth_vert,fragmentShader:ze.depth_frag},normal:{uniforms:an([se.common,se.bumpmap,se.normalmap,se.displacementmap,{opacity:{value:1}}]),vertexShader:ze.meshnormal_vert,fragmentShader:ze.meshnormal_frag},sprite:{uniforms:an([se.sprite,se.fog]),vertexShader:ze.sprite_vert,fragmentShader:ze.sprite_frag},background:{uniforms:{uvTransform:{value:new Ve},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:ze.background_vert,fragmentShader:ze.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Ve}},vertexShader:ze.backgroundCube_vert,fragmentShader:ze.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:ze.cube_vert,fragmentShader:ze.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:ze.equirect_vert,fragmentShader:ze.equirect_frag},distanceRGBA:{uniforms:an([se.common,se.displacementmap,{referencePosition:{value:new O},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:ze.distanceRGBA_vert,fragmentShader:ze.distanceRGBA_frag},shadow:{uniforms:an([se.lights,se.fog,{color:{value:new at(0)},opacity:{value:1}}]),vertexShader:ze.shadow_vert,fragmentShader:ze.shadow_frag}};oi.physical={uniforms:an([oi.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Ve},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Ve},clearcoatNormalScale:{value:new st(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Ve},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Ve},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Ve},sheen:{value:0},sheenColor:{value:new at(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Ve},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Ve},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Ve},transmissionSamplerSize:{value:new st},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Ve},attenuationDistance:{value:0},attenuationColor:{value:new at(0)},specularColor:{value:new at(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Ve},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Ve},anisotropyVector:{value:new st},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Ve}}]),vertexShader:ze.meshphysical_vert,fragmentShader:ze.meshphysical_frag};var Al={r:0,b:0,g:0},Yr=new ao,P1=new Tt;function O1(n,e,t,i,r,s,o){let a=new at(0),c=s===!0?0:1,l,u,d=null,h=0,f=null;function g(S){let E=S.isScene===!0?S.background:null;return E&&E.isTexture&&(E=(S.backgroundBlurriness>0?t:e).get(E)),E}function v(S){let E=!1,b=g(S);b===null?p(a,c):b&&b.isColor&&(p(b,1),E=!0);let F=n.xr.getEnvironmentBlendMode();F==="additive"?i.buffers.color.setClear(0,0,0,1,o):F==="alpha-blend"&&i.buffers.color.setClear(0,0,0,0,o),(n.autoClear||E)&&(i.buffers.depth.setTest(!0),i.buffers.depth.setMask(!0),i.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function m(S,E){let b=g(E);b&&(b.isCubeTexture||b.mapping===uu)?(u===void 0&&(u=new Yt(new _a(1,1,1),new li({name:"BackgroundCubeMaterial",uniforms:co(oi.backgroundCube.uniforms),vertexShader:oi.backgroundCube.vertexShader,fragmentShader:oi.backgroundCube.fragmentShader,side:_n,depthTest:!1,depthWrite:!1,fog:!1})),u.geometry.deleteAttribute("normal"),u.geometry.deleteAttribute("uv"),u.onBeforeRender=function(F,T,C){this.matrixWorld.copyPosition(C.matrixWorld)},Object.defineProperty(u.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),r.update(u)),Yr.copy(E.backgroundRotation),Yr.x*=-1,Yr.y*=-1,Yr.z*=-1,b.isCubeTexture&&b.isRenderTargetTexture===!1&&(Yr.y*=-1,Yr.z*=-1),u.material.uniforms.envMap.value=b,u.material.uniforms.flipEnvMap.value=b.isCubeTexture&&b.isRenderTargetTexture===!1?-1:1,u.material.uniforms.backgroundBlurriness.value=E.backgroundBlurriness,u.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,u.material.uniforms.backgroundRotation.value.setFromMatrix4(P1.makeRotationFromEuler(Yr)),u.material.toneMapped=rt.getTransfer(b.colorSpace)!==ht,(d!==b||h!==b.version||f!==n.toneMapping)&&(u.material.needsUpdate=!0,d=b,h=b.version,f=n.toneMapping),u.layers.enableAll(),S.unshift(u,u.geometry,u.material,0,0,null)):b&&b.isTexture&&(l===void 0&&(l=new Yt(new Jl(2,2),new li({name:"BackgroundMaterial",uniforms:co(oi.background.uniforms),vertexShader:oi.background.vertexShader,fragmentShader:oi.background.fragmentShader,side:fr,depthTest:!1,depthWrite:!1,fog:!1})),l.geometry.deleteAttribute("normal"),Object.defineProperty(l.material,"map",{get:function(){return this.uniforms.t2D.value}}),r.update(l)),l.material.uniforms.t2D.value=b,l.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,l.material.toneMapped=rt.getTransfer(b.colorSpace)!==ht,b.matrixAutoUpdate===!0&&b.updateMatrix(),l.material.uniforms.uvTransform.value.copy(b.matrix),(d!==b||h!==b.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,d=b,h=b.version,f=n.toneMapping),l.layers.enableAll(),S.unshift(l,l.geometry,l.material,0,0,null))}function p(S,E){S.getRGB(Al,yx(n)),i.buffers.color.setClear(Al.r,Al.g,Al.b,E,o)}return{getClearColor:function(){return a},setClearColor:function(S,E=1){a.set(S),c=E,p(a,c)},getClearAlpha:function(){return c},setClearAlpha:function(S){c=S,p(a,c)},render:v,addToRenderList:m}}function F1(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=h(null),s=r,o=!1;function a(x,D,W,B,Z){let J=!1,q=d(B,W,D);s!==q&&(s=q,l(s.object)),J=f(x,B,W,Z),J&&g(x,B,W,Z),Z!==null&&e.update(Z,n.ELEMENT_ARRAY_BUFFER),(J||o)&&(o=!1,b(x,D,W,B),Z!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(Z).buffer))}function c(){return n.createVertexArray()}function l(x){return n.bindVertexArray(x)}function u(x){return n.deleteVertexArray(x)}function d(x,D,W){let B=W.wireframe===!0,Z=i[x.id];Z===void 0&&(Z={},i[x.id]=Z);let J=Z[D.id];J===void 0&&(J={},Z[D.id]=J);let q=J[B];return q===void 0&&(q=h(c()),J[B]=q),q}function h(x){let D=[],W=[],B=[];for(let Z=0;Z<t;Z++)D[Z]=0,W[Z]=0,B[Z]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:D,enabledAttributes:W,attributeDivisors:B,object:x,attributes:{},index:null}}function f(x,D,W,B){let Z=s.attributes,J=D.attributes,q=0,Q=W.getAttributes();for(let z in Q)if(Q[z].location>=0){let he=Z[z],we=J[z];if(we===void 0&&(z==="instanceMatrix"&&x.instanceMatrix&&(we=x.instanceMatrix),z==="instanceColor"&&x.instanceColor&&(we=x.instanceColor)),he===void 0||he.attribute!==we||we&&he.data!==we.data)return!0;q++}return s.attributesNum!==q||s.index!==B}function g(x,D,W,B){let Z={},J=D.attributes,q=0,Q=W.getAttributes();for(let z in Q)if(Q[z].location>=0){let he=J[z];he===void 0&&(z==="instanceMatrix"&&x.instanceMatrix&&(he=x.instanceMatrix),z==="instanceColor"&&x.instanceColor&&(he=x.instanceColor));let we={};we.attribute=he,he&&he.data&&(we.data=he.data),Z[z]=we,q++}s.attributes=Z,s.attributesNum=q,s.index=B}function v(){let x=s.newAttributes;for(let D=0,W=x.length;D<W;D++)x[D]=0}function m(x){p(x,0)}function p(x,D){let W=s.newAttributes,B=s.enabledAttributes,Z=s.attributeDivisors;W[x]=1,B[x]===0&&(n.enableVertexAttribArray(x),B[x]=1),Z[x]!==D&&(n.vertexAttribDivisor(x,D),Z[x]=D)}function S(){let x=s.newAttributes,D=s.enabledAttributes;for(let W=0,B=D.length;W<B;W++)D[W]!==x[W]&&(n.disableVertexAttribArray(W),D[W]=0)}function E(x,D,W,B,Z,J,q){q===!0?n.vertexAttribIPointer(x,D,W,Z,J):n.vertexAttribPointer(x,D,W,B,Z,J)}function b(x,D,W,B){v();let Z=B.attributes,J=W.getAttributes(),q=D.defaultAttributeValues;for(let Q in J){let z=J[Q];if(z.location>=0){let ae=Z[Q];if(ae===void 0&&(Q==="instanceMatrix"&&x.instanceMatrix&&(ae=x.instanceMatrix),Q==="instanceColor"&&x.instanceColor&&(ae=x.instanceColor)),ae!==void 0){let he=ae.normalized,we=ae.itemSize,je=e.get(ae);if(je===void 0)continue;let mt=je.buffer,$=je.type,re=je.bytesPerElement,be=$===n.INT||$===n.UNSIGNED_INT||ae.gpuType===hm;if(ae.isInterleavedBufferAttribute){let ce=ae.data,Re=ce.stride,Le=ae.offset;if(ce.isInstancedInterleavedBuffer){for(let $e=0;$e<z.locationSize;$e++)p(z.location+$e,ce.meshPerAttribute);x.isInstancedMesh!==!0&&B._maxInstanceCount===void 0&&(B._maxInstanceCount=ce.meshPerAttribute*ce.count)}else for(let $e=0;$e<z.locationSize;$e++)m(z.location+$e);n.bindBuffer(n.ARRAY_BUFFER,mt);for(let $e=0;$e<z.locationSize;$e++)E(z.location+$e,we/z.locationSize,$,he,Re*re,(Le+we/z.locationSize*$e)*re,be)}else{if(ae.isInstancedBufferAttribute){for(let ce=0;ce<z.locationSize;ce++)p(z.location+ce,ae.meshPerAttribute);x.isInstancedMesh!==!0&&B._maxInstanceCount===void 0&&(B._maxInstanceCount=ae.meshPerAttribute*ae.count)}else for(let ce=0;ce<z.locationSize;ce++)m(z.location+ce);n.bindBuffer(n.ARRAY_BUFFER,mt);for(let ce=0;ce<z.locationSize;ce++)E(z.location+ce,we/z.locationSize,$,he,we*re,we/z.locationSize*ce*re,be)}}else if(q!==void 0){let he=q[Q];if(he!==void 0)switch(he.length){case 2:n.vertexAttrib2fv(z.location,he);break;case 3:n.vertexAttrib3fv(z.location,he);break;case 4:n.vertexAttrib4fv(z.location,he);break;default:n.vertexAttrib1fv(z.location,he)}}}}S()}function F(){I();for(let x in i){let D=i[x];for(let W in D){let B=D[W];for(let Z in B)u(B[Z].object),delete B[Z];delete D[W]}delete i[x]}}function T(x){if(i[x.id]===void 0)return;let D=i[x.id];for(let W in D){let B=D[W];for(let Z in B)u(B[Z].object),delete B[Z];delete D[W]}delete i[x.id]}function C(x){for(let D in i){let W=i[D];if(W[x.id]===void 0)continue;let B=W[x.id];for(let Z in B)u(B[Z].object),delete B[Z];delete W[x.id]}}function I(){M(),o=!0,s!==r&&(s=r,l(s.object))}function M(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:I,resetDefaultState:M,dispose:F,releaseStatesOfGeometry:T,releaseStatesOfProgram:C,initAttributes:v,enableAttribute:m,disableUnusedAttributes:S}}function L1(n,e,t){let i;function r(l){i=l}function s(l,u){n.drawArrays(i,l,u),t.update(u,i,1)}function o(l,u,d){d!==0&&(n.drawArraysInstanced(i,l,u,d),t.update(u,i,d))}function a(l,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,l,0,u,0,d);let f=0;for(let g=0;g<d;g++)f+=u[g];t.update(f,i,1)}function c(l,u,d,h){if(d===0)return;let f=e.get("WEBGL_multi_draw");if(f===null)for(let g=0;g<l.length;g++)o(l[g],u[g],h[g]);else{f.multiDrawArraysInstancedWEBGL(i,l,0,u,0,h,0,d);let g=0;for(let v=0;v<d;v++)g+=u[v]*h[v];t.update(g,i,1)}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=c}function k1(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let C=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(C.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(C){return!(C!==Yn&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(C){let I=C===Ma&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(C!==Gi&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&C!==Bi&&!I)}function c(C){if(C==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";C="mediump"}return C==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(console.warn("THREE.WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,h=t.reverseDepthBuffer===!0&&e.has("EXT_clip_control"),f=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),v=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),S=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),E=n.getParameter(n.MAX_VARYING_VECTORS),b=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),F=g>0,T=n.getParameter(n.MAX_SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reverseDepthBuffer:h,maxTextures:f,maxVertexTextures:g,maxTextureSize:v,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:S,maxVaryings:E,maxFragmentUniforms:b,vertexTextures:F,maxSamples:T}}function U1(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Ui,a=new Ve,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,h){let f=d.length!==0||h||i!==0||r;return r=h,i=d.length,f},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,h){t=u(d,h,0)},this.setState=function(d,h,f){let g=d.clippingPlanes,v=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let S=s?0:i,E=S*4,b=p.clippingState||null;c.value=b,b=u(g,h,E,f);for(let F=0;F!==E;++F)b[F]=t[F];p.clippingState=b,this.numIntersection=v?this.numPlanes:0,this.numPlanes+=S}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,h,f,g){let v=d!==null?d.length:0,m=null;if(v!==0){if(m=c.value,g!==!0||m===null){let p=f+v*4,S=h.matrixWorldInverse;a.getNormalMatrix(S),(m===null||m.length<p)&&(m=new Float32Array(p));for(let E=0,b=f;E!==v;++E,b+=4)o.copy(d[E]).applyMatrix4(S,a),o.normal.toArray(m,b),m[b+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=v,e.numIntersection=0,m}}function V1(n){let e=new WeakMap;function t(o,a){return a===op?o.mapping=no:a===ap&&(o.mapping=io),o}function i(o){if(o&&o.isTexture){let a=o.mapping;if(a===op||a===ap)if(e.has(o)){let c=e.get(o).texture;return t(c,o.mapping)}else{let c=o.image;if(c&&c.height>0){let l=new zp(c.height);return l.fromEquirectangularTexture(n,o),e.set(o,l),o.addEventListener("dispose",r),t(l.texture,o.mapping)}else return null}}return o}function r(o){let a=o.target;a.removeEventListener("dispose",r);let c=e.get(a);c!==void 0&&(e.delete(a),c.dispose())}function s(){e=new WeakMap}return{get:i,dispose:s}}var Gp=class extends Yl{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},Ys=4,T0=[.125,.215,.35,.446,.526,.582],es=20,Gh=new Gp,D0=new at,Wh=null,jh=0,$h=0,qh=!1,Jr=(1+Math.sqrt(5))/2,Xs=1/Jr,A0=[new O(-Jr,Xs,0),new O(Jr,Xs,0),new O(-Xs,0,Jr),new O(Xs,0,Jr),new O(0,Jr,-Xs),new O(0,Jr,Xs),new O(-1,1,-1),new O(1,1,-1),new O(-1,1,1),new O(1,1,1)],Kl=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._lodPlanes=[],this._sizeLods=[],this._sigmas=[],this._blurMaterial=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._compileMaterial(this._blurMaterial)}fromScene(e,t=0,i=.1,r=100){Wh=this._renderer.getRenderTarget(),jh=this._renderer.getActiveCubeFace(),$h=this._renderer.getActiveMipmapLevel(),qh=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(256);let s=this._allocateTargets();return s.depthBuffer=!0,this._sceneToCubeUV(e,i,r,s),t>0&&this._blur(s,0,0,t),this._applyPMREM(s),this._cleanup(s),s}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=N0(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=R0(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose()}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodPlanes.length;e++)this._lodPlanes[e].dispose()}_cleanup(e){this._renderer.setRenderTarget(Wh,jh,$h),this._renderer.xr.enabled=qh,e.scissorTest=!1,Il(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===no||e.mapping===io?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Wh=this._renderer.getRenderTarget(),jh=this._renderer.getActiveCubeFace(),$h=this._renderer.getActiveMipmapLevel(),qh=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:ai,minFilter:ai,generateMipmaps:!1,type:Ma,format:Yn,colorSpace:fo,depthBuffer:!1},r=I0(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=I0(e,t,i);let{_lodMax:s}=this;({sizeLods:this._sizeLods,lodPlanes:this._lodPlanes,sigmas:this._sigmas}=B1(s)),this._blurMaterial=H1(s,e,t)}return r}_compileMaterial(e){let t=new Yt(this._lodPlanes[0],e);this._renderer.compile(t,Gh)}_sceneToCubeUV(e,t,i,r){let a=new nn(90,1,t,i),c=[1,-1,1,1,1,1],l=[1,1,1,-1,-1,-1],u=this._renderer,d=u.autoClear,h=u.toneMapping;u.getClearColor(D0),u.toneMapping=dr,u.autoClear=!1;let f=new ci({name:"PMREM.Background",side:_n,depthWrite:!1,depthTest:!1}),g=new Yt(new _a,f),v=!1,m=e.background;m?m.isColor&&(f.color.copy(m),e.background=null,v=!0):(f.color.copy(D0),v=!0);for(let p=0;p<6;p++){let S=p%3;S===0?(a.up.set(0,c[p],0),a.lookAt(l[p],0,0)):S===1?(a.up.set(0,0,c[p]),a.lookAt(0,l[p],0)):(a.up.set(0,c[p],0),a.lookAt(0,0,l[p]));let E=this._cubeSize;Il(r,S*E,p>2?E:0,E,E),u.setRenderTarget(r),v&&u.render(g,a),u.render(e,a)}g.geometry.dispose(),g.material.dispose(),u.toneMapping=h,u.autoClear=d,e.background=m}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===no||e.mapping===io;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=N0()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=R0());let s=r?this._cubemapMaterial:this._equirectMaterial,o=new Yt(this._lodPlanes[0],s),a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Il(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,Gh)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodPlanes.length;for(let s=1;s<r;s++){let o=Math.sqrt(this._sigmas[s]*this._sigmas[s]-this._sigmas[s-1]*this._sigmas[s-1]),a=A0[(r-s-1)%A0.length];this._blur(e,s-1,s,o,a)}t.autoClear=i}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&console.error("blur direction must be either latitudinal or longitudinal!");let u=3,d=new Yt(this._lodPlanes[r],l),h=l.uniforms,f=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*f):2*Math.PI/(2*es-1),v=s/g,m=isFinite(s)?1+Math.floor(u*v):es;m>es&&console.warn(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${es}`);let p=[],S=0;for(let C=0;C<es;++C){let I=C/v,M=Math.exp(-I*I/2);p.push(M),C===0?S+=M:C<m&&(S+=2*M)}for(let C=0;C<p.length;C++)p[C]=p[C]/S;h.envMap.value=e.texture,h.samples.value=m,h.weights.value=p,h.latitudinal.value=o==="latitudinal",a&&(h.poleAxis.value=a);let{_lodMax:E}=this;h.dTheta.value=g,h.mipInt.value=E-i;let b=this._sizeLods[r],F=3*b*(r>E-Ys?r-E+Ys:0),T=4*(this._cubeSize-b);Il(t,F,T,3*b,2*b),c.setRenderTarget(t),c.render(d,Gh)}};function B1(n){let e=[],t=[],i=[],r=n,s=n-Ys+1+T0.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);t.push(a);let c=1/a;o>n-Ys?c=T0[o-n+Ys-1]:o===0&&(c=0),i.push(c);let l=1/(a-2),u=-l,d=1+l,h=[u,u,d,u,d,d,u,u,d,d,u,d],f=6,g=6,v=3,m=2,p=1,S=new Float32Array(v*g*f),E=new Float32Array(m*g*f),b=new Float32Array(p*g*f);for(let T=0;T<f;T++){let C=T%3*2/3-1,I=T>2?0:-1,M=[C,I,0,C+2/3,I,0,C+2/3,I+1,0,C,I,0,C+2/3,I+1,0,C,I+1,0];S.set(M,v*g*T),E.set(h,m*g*T);let x=[T,T,T,T,T,T];b.set(x,p*g*T)}let F=new wn;F.setAttribute("position",new xn(S,v)),F.setAttribute("uv",new xn(E,m)),F.setAttribute("faceIndex",new xn(b,p)),e.push(F),r>Ys&&r--}return{lodPlanes:e,sizeLods:t,sigmas:i}}function I0(n,e,t){let i=new Wi(n,e,t);return i.texture.mapping=uu,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Il(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function H1(n,e,t){let i=new Float32Array(es),r=new O(0,1,0);return new li({name:"SphericalGaussianBlur",defines:{n:es,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:_m(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:ur,depthTest:!1,depthWrite:!1})}function R0(){return new li({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:_m(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:ur,depthTest:!1,depthWrite:!1})}function N0(){return new li({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:_m(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:ur,depthTest:!1,depthWrite:!1})}function _m(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}function z1(n){let e=new WeakMap,t=null;function i(a){if(a&&a.isTexture){let c=a.mapping,l=c===op||c===ap,u=c===no||c===io;if(l||u){let d=e.get(a),h=d!==void 0?d.texture.pmremVersion:0;if(a.isRenderTargetTexture&&a.pmremVersion!==h)return t===null&&(t=new Kl(n)),d=l?t.fromEquirectangular(a,d):t.fromCubemap(a,d),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),d.texture;if(d!==void 0)return d.texture;{let f=a.image;return l&&f&&f.height>0||u&&f&&r(f)?(t===null&&(t=new Kl(n)),d=l?t.fromEquirectangular(a):t.fromCubemap(a),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),a.addEventListener("dispose",s),d.texture):null}}}return a}function r(a){let c=0,l=6;for(let u=0;u<l;u++)a[u]!==void 0&&c++;return c===l}function s(a){let c=a.target;c.removeEventListener("dispose",s);let l=e.get(c);l!==void 0&&(e.delete(c),l.dispose())}function o(){e=new WeakMap,t!==null&&(t.dispose(),t=null)}return{get:i,dispose:o}}function G1(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r;switch(i){case"WEBGL_depth_texture":r=n.getExtension("WEBGL_depth_texture")||n.getExtension("MOZ_WEBGL_depth_texture")||n.getExtension("WEBKIT_WEBGL_depth_texture");break;case"EXT_texture_filter_anisotropic":r=n.getExtension("EXT_texture_filter_anisotropic")||n.getExtension("MOZ_EXT_texture_filter_anisotropic")||n.getExtension("WEBKIT_EXT_texture_filter_anisotropic");break;case"WEBGL_compressed_texture_s3tc":r=n.getExtension("WEBGL_compressed_texture_s3tc")||n.getExtension("MOZ_WEBGL_compressed_texture_s3tc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_s3tc");break;case"WEBGL_compressed_texture_pvrtc":r=n.getExtension("WEBGL_compressed_texture_pvrtc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_pvrtc");break;default:r=n.getExtension(i)}return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&ma("THREE.WebGLRenderer: "+i+" extension not supported."),r}}}function W1(n,e,t,i){let r={},s=new WeakMap;function o(d){let h=d.target;h.index!==null&&e.remove(h.index);for(let g in h.attributes)e.remove(h.attributes[g]);for(let g in h.morphAttributes){let v=h.morphAttributes[g];for(let m=0,p=v.length;m<p;m++)e.remove(v[m])}h.removeEventListener("dispose",o),delete r[h.id];let f=s.get(h);f&&(e.remove(f),s.delete(h)),i.releaseStatesOfGeometry(h),h.isInstancedBufferGeometry===!0&&delete h._maxInstanceCount,t.memory.geometries--}function a(d,h){return r[h.id]===!0||(h.addEventListener("dispose",o),r[h.id]=!0,t.memory.geometries++),h}function c(d){let h=d.attributes;for(let g in h)e.update(h[g],n.ARRAY_BUFFER);let f=d.morphAttributes;for(let g in f){let v=f[g];for(let m=0,p=v.length;m<p;m++)e.update(v[m],n.ARRAY_BUFFER)}}function l(d){let h=[],f=d.index,g=d.attributes.position,v=0;if(f!==null){let S=f.array;v=f.version;for(let E=0,b=S.length;E<b;E+=3){let F=S[E+0],T=S[E+1],C=S[E+2];h.push(F,T,T,C,C,F)}}else if(g!==void 0){let S=g.array;v=g.version;for(let E=0,b=S.length/3-1;E<b;E+=3){let F=E+0,T=E+1,C=E+2;h.push(F,T,T,C,C,F)}}else return;let m=new(gx(h)?Xl:ql)(h,1);m.version=v;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let h=s.get(d);if(h){let f=d.index;f!==null&&h.version<f.version&&l(d)}else l(d);return s.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function j1(n,e,t){let i;function r(h){i=h}let s,o;function a(h){s=h.type,o=h.bytesPerElement}function c(h,f){n.drawElements(i,f,s,h*o),t.update(f,i,1)}function l(h,f,g){g!==0&&(n.drawElementsInstanced(i,f,s,h*o,g),t.update(f,i,g))}function u(h,f,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,h,0,g);let m=0;for(let p=0;p<g;p++)m+=f[p];t.update(m,i,1)}function d(h,f,g,v){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<h.length;p++)l(h[p]/o,f[p],v[p]);else{m.multiDrawElementsInstancedWEBGL(i,f,0,s,h,0,v,0,g);let p=0;for(let S=0;S<g;S++)p+=f[S]*v[S];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function $1(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:console.error("THREE.WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function q1(n,e,t){let i=new WeakMap,r=new pt;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,h=i.get(a);if(h===void 0||h.count!==d){let x=function(){I.dispose(),i.delete(a),a.removeEventListener("dispose",x)};var f=x;h!==void 0&&h.texture.dispose();let g=a.morphAttributes.position!==void 0,v=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],S=a.morphAttributes.normal||[],E=a.morphAttributes.color||[],b=0;g===!0&&(b=1),v===!0&&(b=2),m===!0&&(b=3);let F=a.attributes.position.count*b,T=1;F>e.maxTextureSize&&(T=Math.ceil(F/e.maxTextureSize),F=e.maxTextureSize);let C=new Float32Array(F*T*4*d),I=new Wl(C,F,T,d);I.type=Bi,I.needsUpdate=!0;let M=b*4;for(let D=0;D<d;D++){let W=p[D],B=S[D],Z=E[D],J=F*T*4*D;for(let q=0;q<W.count;q++){let Q=q*M;g===!0&&(r.fromBufferAttribute(W,q),C[J+Q+0]=r.x,C[J+Q+1]=r.y,C[J+Q+2]=r.z,C[J+Q+3]=0),v===!0&&(r.fromBufferAttribute(B,q),C[J+Q+4]=r.x,C[J+Q+5]=r.y,C[J+Q+6]=r.z,C[J+Q+7]=0),m===!0&&(r.fromBufferAttribute(Z,q),C[J+Q+8]=r.x,C[J+Q+9]=r.y,C[J+Q+10]=r.z,C[J+Q+11]=Z.itemSize===4?r.w:1)}}h={count:d,texture:I,size:new st(F,T)},i.set(a,h),a.addEventListener("dispose",x)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let v=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",v),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",h.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",h.size)}return{update:s}}function X1(n,e,t,i){let r=new WeakMap;function s(c){let l=i.render.frame,u=c.geometry,d=e.get(c,u);if(r.get(d)!==l&&(e.update(d),r.set(d,l)),c.isInstancedMesh&&(c.hasEventListener("dispose",a)===!1&&c.addEventListener("dispose",a),r.get(c)!==l&&(t.update(c.instanceMatrix,n.ARRAY_BUFFER),c.instanceColor!==null&&t.update(c.instanceColor,n.ARRAY_BUFFER),r.set(c,l))),c.isSkinnedMesh){let h=c.skeleton;r.get(h)!==l&&(h.update(),r.set(h,l))}return d}function o(){r=new WeakMap}function a(c){let l=c.target;l.removeEventListener("dispose",a),t.remove(l.instanceMatrix),l.instanceColor!==null&&t.remove(l.instanceColor)}return{update:s,dispose:o}}var Ql=class extends cs{constructor(e,t,i,r,s,o,a,c,l,u=Ks){if(u!==Ks&&u!==so)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");i===void 0&&u===Ks&&(i=is),i===void 0&&u===so&&(i=ro),super(null,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.image={width:e,height:t},this.magFilter=a!==void 0?a:Zn,this.minFilter=c!==void 0?c:Zn,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},xx=new cs,P0=new Ql(1,1),bx=new Wl,Mx=new Bp,Ex=new Zl,O0=[],F0=[],L0=new Float32Array(16),k0=new Float32Array(9),U0=new Float32Array(4);function ho(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=O0[r];if(s===void 0&&(s=new Float32Array(r),O0[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Ht(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function zt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function fu(n,e){let t=F0[e];t===void 0&&(t=new Int32Array(e),F0[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function Y1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function Z1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ht(t,e))return;n.uniform2fv(this.addr,e),zt(t,e)}}function J1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Ht(t,e))return;n.uniform3fv(this.addr,e),zt(t,e)}}function K1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ht(t,e))return;n.uniform4fv(this.addr,e),zt(t,e)}}function Q1(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ht(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),zt(t,e)}else{if(Ht(t,i))return;U0.set(i),n.uniformMatrix2fv(this.addr,!1,U0),zt(t,i)}}function eR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ht(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),zt(t,e)}else{if(Ht(t,i))return;k0.set(i),n.uniformMatrix3fv(this.addr,!1,k0),zt(t,i)}}function tR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ht(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),zt(t,e)}else{if(Ht(t,i))return;L0.set(i),n.uniformMatrix4fv(this.addr,!1,L0),zt(t,i)}}function nR(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function iR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ht(t,e))return;n.uniform2iv(this.addr,e),zt(t,e)}}function rR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ht(t,e))return;n.uniform3iv(this.addr,e),zt(t,e)}}function sR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ht(t,e))return;n.uniform4iv(this.addr,e),zt(t,e)}}function oR(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function aR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ht(t,e))return;n.uniform2uiv(this.addr,e),zt(t,e)}}function cR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ht(t,e))return;n.uniform3uiv(this.addr,e),zt(t,e)}}function lR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ht(t,e))return;n.uniform4uiv(this.addr,e),zt(t,e)}}function uR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(P0.compareFunction=mx,s=P0):s=xx,t.setTexture2D(e||s,r)}function dR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||Mx,r)}function fR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||Ex,r)}function hR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||bx,r)}function pR(n){switch(n){case 5126:return Y1;case 35664:return Z1;case 35665:return J1;case 35666:return K1;case 35674:return Q1;case 35675:return eR;case 35676:return tR;case 5124:case 35670:return nR;case 35667:case 35671:return iR;case 35668:case 35672:return rR;case 35669:case 35673:return sR;case 5125:return oR;case 36294:return aR;case 36295:return cR;case 36296:return lR;case 35678:case 36198:case 36298:case 36306:case 35682:return uR;case 35679:case 36299:case 36307:return dR;case 35680:case 36300:case 36308:case 36293:return fR;case 36289:case 36303:case 36311:case 36292:return hR}}function mR(n,e){n.uniform1fv(this.addr,e)}function gR(n,e){let t=ho(e,this.size,2);n.uniform2fv(this.addr,t)}function vR(n,e){let t=ho(e,this.size,3);n.uniform3fv(this.addr,t)}function yR(n,e){let t=ho(e,this.size,4);n.uniform4fv(this.addr,t)}function _R(n,e){let t=ho(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function xR(n,e){let t=ho(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function bR(n,e){let t=ho(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function MR(n,e){n.uniform1iv(this.addr,e)}function ER(n,e){n.uniform2iv(this.addr,e)}function SR(n,e){n.uniform3iv(this.addr,e)}function wR(n,e){n.uniform4iv(this.addr,e)}function CR(n,e){n.uniform1uiv(this.addr,e)}function TR(n,e){n.uniform2uiv(this.addr,e)}function DR(n,e){n.uniform3uiv(this.addr,e)}function AR(n,e){n.uniform4uiv(this.addr,e)}function IR(n,e,t){let i=this.cache,r=e.length,s=fu(t,r);Ht(i,s)||(n.uniform1iv(this.addr,s),zt(i,s));for(let o=0;o!==r;++o)t.setTexture2D(e[o]||xx,s[o])}function RR(n,e,t){let i=this.cache,r=e.length,s=fu(t,r);Ht(i,s)||(n.uniform1iv(this.addr,s),zt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||Mx,s[o])}function NR(n,e,t){let i=this.cache,r=e.length,s=fu(t,r);Ht(i,s)||(n.uniform1iv(this.addr,s),zt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||Ex,s[o])}function PR(n,e,t){let i=this.cache,r=e.length,s=fu(t,r);Ht(i,s)||(n.uniform1iv(this.addr,s),zt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||bx,s[o])}function OR(n){switch(n){case 5126:return mR;case 35664:return gR;case 35665:return vR;case 35666:return yR;case 35674:return _R;case 35675:return xR;case 35676:return bR;case 5124:case 35670:return MR;case 35667:case 35671:return ER;case 35668:case 35672:return SR;case 35669:case 35673:return wR;case 5125:return CR;case 36294:return TR;case 36295:return DR;case 36296:return AR;case 35678:case 36198:case 36298:case 36306:case 35682:return IR;case 35679:case 36299:case 36307:return RR;case 35680:case 36300:case 36308:case 36293:return NR;case 36289:case 36303:case 36311:case 36292:return PR}}var Wp=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=pR(t.type)}},jp=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=OR(t.type)}},$p=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Xh=/(\w+)(\])?(\[|\.)?/g;function V0(n,e){n.seq.push(e),n.map[e.id]=e}function FR(n,e,t){let i=n.name,r=i.length;for(Xh.lastIndex=0;;){let s=Xh.exec(i),o=Xh.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){V0(t,l===void 0?new Wp(a,n,e):new jp(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new $p(a),V0(t,d)),t=d}}}var eo=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let r=0;r<i;++r){let s=e.getActiveUniform(t,r),o=e.getUniformLocation(t,s.name);FR(s,o,this)}}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function B0(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var LR=37297,kR=0;function UR(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var H0=new Ve;function VR(n){rt._getMatrix(H0,rt.workingColorSpace,n);let e=`mat3( ${H0.elements.map(t=>t.toFixed(4))} )`;switch(rt.getTransfer(n)){case du:return[e,"LinearTransferOETF"];case ht:return[e,"sRGBTransferOETF"];default:return console.warn("THREE.WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function z0(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),r=n.getShaderInfoLog(e).trim();if(i&&r==="")return"";let s=/ERROR: 0:(\d+)/.exec(r);if(s){let o=parseInt(s[1]);return t.toUpperCase()+`

`+r+`

`+UR(n.getShaderSource(e),o)}else return r}function BR(n,e){let t=VR(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}function HR(n,e){let t;switch(e){case dD:t="Linear";break;case fD:t="Reinhard";break;case hD:t="Cineon";break;case pD:t="ACESFilmic";break;case gD:t="AgX";break;case vD:t="Neutral";break;case mD:t="Custom";break;default:console.warn("THREE.WebGLProgram: Unsupported toneMapping:",e),t="Linear"}return"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Rl=new O;function zR(){rt.getLuminanceCoefficients(Rl);let n=Rl.x.toFixed(4),e=Rl.y.toFixed(4),t=Rl.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function GR(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(ga).join(`
`)}function WR(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function jR(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function ga(n){return n!==""}function G0(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function W0(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var $R=/^[ \t]*#include +<([\w\d./]+)>/gm;function qp(n){return n.replace($R,XR)}var qR=new Map;function XR(n,e){let t=ze[e];if(t===void 0){let i=qR.get(e);if(i!==void 0)t=ze[i],console.warn('THREE.WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return qp(t)}var YR=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function j0(n){return n.replace(YR,ZR)}function ZR(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function $0(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}function JR(n){let e="SHADOWMAP_TYPE_BASIC";return n.shadowMapType===ix?e="SHADOWMAP_TYPE_PCF":n.shadowMapType===WT?e="SHADOWMAP_TYPE_PCF_SOFT":n.shadowMapType===ki&&(e="SHADOWMAP_TYPE_VSM"),e}function KR(n){let e="ENVMAP_TYPE_CUBE";if(n.envMap)switch(n.envMapMode){case no:case io:e="ENVMAP_TYPE_CUBE";break;case uu:e="ENVMAP_TYPE_CUBE_UV";break}return e}function QR(n){let e="ENVMAP_MODE_REFLECTION";return n.envMap&&n.envMapMode===io&&(e="ENVMAP_MODE_REFRACTION"),e}function eN(n){let e="ENVMAP_BLENDING_NONE";if(n.envMap)switch(n.combine){case rx:e="ENVMAP_BLENDING_MULTIPLY";break;case lD:e="ENVMAP_BLENDING_MIX";break;case uD:e="ENVMAP_BLENDING_ADD";break}return e}function tN(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function nN(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=JR(t),l=KR(t),u=QR(t),d=eN(t),h=tN(t),f=GR(t),g=WR(s),v=r.createProgram(),m,p,S=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(ga).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(ga).join(`
`),p.length>0&&(p+=`
`)):(m=[$0(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(ga).join(`
`),p=[$0(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",h?"#define CUBEUV_TEXEL_WIDTH "+h.texelWidth:"",h?"#define CUBEUV_TEXEL_HEIGHT "+h.texelHeight:"",h?"#define CUBEUV_MAX_MIP "+h.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor||t.batchingColor?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==dr?"#define TONE_MAPPING":"",t.toneMapping!==dr?ze.tonemapping_pars_fragment:"",t.toneMapping!==dr?HR("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",ze.colorspace_pars_fragment,BR("linearToOutputTexel",t.outputColorSpace),zR(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(ga).join(`
`)),o=qp(o),o=G0(o,t),o=W0(o,t),a=qp(a),a=G0(a,t),a=W0(a,t),o=j0(o),a=j0(a),t.isRawShaderMaterial!==!0&&(S=`#version 300 es
`,m=[f,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===o0?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===o0?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let E=S+m+o,b=S+p+a,F=B0(r,r.VERTEX_SHADER,E),T=B0(r,r.FRAGMENT_SHADER,b);r.attachShader(v,F),r.attachShader(v,T),t.index0AttributeName!==void 0?r.bindAttribLocation(v,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(v,0,"position"),r.linkProgram(v);function C(D){if(n.debug.checkShaderErrors){let W=r.getProgramInfoLog(v).trim(),B=r.getShaderInfoLog(F).trim(),Z=r.getShaderInfoLog(T).trim(),J=!0,q=!0;if(r.getProgramParameter(v,r.LINK_STATUS)===!1)if(J=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,v,F,T);else{let Q=z0(r,F,"vertex"),z=z0(r,T,"fragment");console.error("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(v,r.VALIDATE_STATUS)+`

Material Name: `+D.name+`
Material Type: `+D.type+`

Program Info Log: `+W+`
`+Q+`
`+z)}else W!==""?console.warn("THREE.WebGLProgram: Program Info Log:",W):(B===""||Z==="")&&(q=!1);q&&(D.diagnostics={runnable:J,programLog:W,vertexShader:{log:B,prefix:m},fragmentShader:{log:Z,prefix:p}})}r.deleteShader(F),r.deleteShader(T),I=new eo(r,v),M=jR(r,v)}let I;this.getUniforms=function(){return I===void 0&&C(this),I};let M;this.getAttributes=function(){return M===void 0&&C(this),M};let x=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return x===!1&&(x=r.getProgramParameter(v,LR)),x},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(v),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=kR++,this.cacheKey=e,this.usedTimes=1,this.program=v,this.vertexShader=F,this.fragmentShader=T,this}var iN=0,Xp=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Yp(e),t.set(e,i)),i}},Yp=class{constructor(e){this.id=iN++,this.code=e,this.usedTimes=0}};function rN(n,e,t,i,r,s,o){let a=new $l,c=new Xp,l=new Set,u=[],d=r.logarithmicDepthBuffer,h=r.vertexTextures,f=r.precision,g={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distanceRGBA",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function v(M){return l.add(M),M===0?"uv":`uv${M}`}function m(M,x,D,W,B){let Z=W.fog,J=B.geometry,q=M.isMeshStandardMaterial?W.environment:null,Q=(M.isMeshStandardMaterial?t:e).get(M.envMap||q),z=Q&&Q.mapping===uu?Q.image.height:null,ae=g[M.type];M.precision!==null&&(f=r.getMaxPrecision(M.precision),f!==M.precision&&console.warn("THREE.WebGLProgram.getParameters:",M.precision,"not supported, using",f,"instead."));let he=J.morphAttributes.position||J.morphAttributes.normal||J.morphAttributes.color,we=he!==void 0?he.length:0,je=0;J.morphAttributes.position!==void 0&&(je=1),J.morphAttributes.normal!==void 0&&(je=2),J.morphAttributes.color!==void 0&&(je=3);let mt,$,re,be;if(ae){let ut=oi[ae];mt=ut.vertexShader,$=ut.fragmentShader}else mt=M.vertexShader,$=M.fragmentShader,c.update(M),re=c.getVertexShaderID(M),be=c.getFragmentShaderID(M);let ce=n.getRenderTarget(),Re=n.state.buffers.depth.getReversed(),Le=B.isInstancedMesh===!0,$e=B.isBatchedMesh===!0,St=!!M.map,nt=!!M.matcap,Nt=!!Q,P=!!M.aoMap,Cn=!!M.lightMap,Ke=!!M.bumpMap,Qe=!!M.normalMap,De=!!M.displacementMap,xt=!!M.emissiveMap,Ce=!!M.metalnessMap,w=!!M.roughnessMap,y=M.anisotropy>0,L=M.clearcoat>0,X=M.dispersion>0,K=M.iridescence>0,j=M.sheen>0,Me=M.transmission>0,le=y&&!!M.anisotropyMap,pe=L&&!!M.clearcoatMap,it=L&&!!M.clearcoatNormalMap,te=L&&!!M.clearcoatRoughnessMap,me=K&&!!M.iridescenceMap,Ae=K&&!!M.iridescenceThicknessMap,Pe=j&&!!M.sheenColorMap,ge=j&&!!M.sheenRoughnessMap,et=!!M.specularMap,Be=!!M.specularColorMap,gt=!!M.specularIntensityMap,A=Me&&!!M.transmissionMap,oe=Me&&!!M.thicknessMap,H=!!M.gradientMap,Y=!!M.alphaMap,fe=M.alphaTest>0,ue=!!M.alphaHash,ke=!!M.extensions,Dt=dr;M.toneMapped&&(ce===null||ce.isXRRenderTarget===!0)&&(Dt=n.toneMapping);let Zt={shaderID:ae,shaderType:M.type,shaderName:M.name,vertexShader:mt,fragmentShader:$,defines:M.defines,customVertexShaderID:re,customFragmentShaderID:be,isRawShaderMaterial:M.isRawShaderMaterial===!0,glslVersion:M.glslVersion,precision:f,batching:$e,batchingColor:$e&&B._colorsTexture!==null,instancing:Le,instancingColor:Le&&B.instanceColor!==null,instancingMorph:Le&&B.morphTexture!==null,supportsVertexTextures:h,outputColorSpace:ce===null?n.outputColorSpace:ce.isXRRenderTarget===!0?ce.texture.colorSpace:fo,alphaToCoverage:!!M.alphaToCoverage,map:St,matcap:nt,envMap:Nt,envMapMode:Nt&&Q.mapping,envMapCubeUVHeight:z,aoMap:P,lightMap:Cn,bumpMap:Ke,normalMap:Qe,displacementMap:h&&De,emissiveMap:xt,normalMapObjectSpace:Qe&&M.normalMapType===MD,normalMapTangentSpace:Qe&&M.normalMapType===bD,metalnessMap:Ce,roughnessMap:w,anisotropy:y,anisotropyMap:le,clearcoat:L,clearcoatMap:pe,clearcoatNormalMap:it,clearcoatRoughnessMap:te,dispersion:X,iridescence:K,iridescenceMap:me,iridescenceThicknessMap:Ae,sheen:j,sheenColorMap:Pe,sheenRoughnessMap:ge,specularMap:et,specularColorMap:Be,specularIntensityMap:gt,transmission:Me,transmissionMap:A,thicknessMap:oe,gradientMap:H,opaque:M.transparent===!1&&M.blending===Js&&M.alphaToCoverage===!1,alphaMap:Y,alphaTest:fe,alphaHash:ue,combine:M.combine,mapUv:St&&v(M.map.channel),aoMapUv:P&&v(M.aoMap.channel),lightMapUv:Cn&&v(M.lightMap.channel),bumpMapUv:Ke&&v(M.bumpMap.channel),normalMapUv:Qe&&v(M.normalMap.channel),displacementMapUv:De&&v(M.displacementMap.channel),emissiveMapUv:xt&&v(M.emissiveMap.channel),metalnessMapUv:Ce&&v(M.metalnessMap.channel),roughnessMapUv:w&&v(M.roughnessMap.channel),anisotropyMapUv:le&&v(M.anisotropyMap.channel),clearcoatMapUv:pe&&v(M.clearcoatMap.channel),clearcoatNormalMapUv:it&&v(M.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:te&&v(M.clearcoatRoughnessMap.channel),iridescenceMapUv:me&&v(M.iridescenceMap.channel),iridescenceThicknessMapUv:Ae&&v(M.iridescenceThicknessMap.channel),sheenColorMapUv:Pe&&v(M.sheenColorMap.channel),sheenRoughnessMapUv:ge&&v(M.sheenRoughnessMap.channel),specularMapUv:et&&v(M.specularMap.channel),specularColorMapUv:Be&&v(M.specularColorMap.channel),specularIntensityMapUv:gt&&v(M.specularIntensityMap.channel),transmissionMapUv:A&&v(M.transmissionMap.channel),thicknessMapUv:oe&&v(M.thicknessMap.channel),alphaMapUv:Y&&v(M.alphaMap.channel),vertexTangents:!!J.attributes.tangent&&(Qe||y),vertexColors:M.vertexColors,vertexAlphas:M.vertexColors===!0&&!!J.attributes.color&&J.attributes.color.itemSize===4,pointsUvs:B.isPoints===!0&&!!J.attributes.uv&&(St||Y),fog:!!Z,useFog:M.fog===!0,fogExp2:!!Z&&Z.isFogExp2,flatShading:M.flatShading===!0,sizeAttenuation:M.sizeAttenuation===!0,logarithmicDepthBuffer:d,reverseDepthBuffer:Re,skinning:B.isSkinnedMesh===!0,morphTargets:J.morphAttributes.position!==void 0,morphNormals:J.morphAttributes.normal!==void 0,morphColors:J.morphAttributes.color!==void 0,morphTargetsCount:we,morphTextureStride:je,numDirLights:x.directional.length,numPointLights:x.point.length,numSpotLights:x.spot.length,numSpotLightMaps:x.spotLightMap.length,numRectAreaLights:x.rectArea.length,numHemiLights:x.hemi.length,numDirLightShadows:x.directionalShadowMap.length,numPointLightShadows:x.pointShadowMap.length,numSpotLightShadows:x.spotShadowMap.length,numSpotLightShadowsWithMaps:x.numSpotLightShadowsWithMaps,numLightProbes:x.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:M.dithering,shadowMapEnabled:n.shadowMap.enabled&&D.length>0,shadowMapType:n.shadowMap.type,toneMapping:Dt,decodeVideoTexture:St&&M.map.isVideoTexture===!0&&rt.getTransfer(M.map.colorSpace)===ht,decodeVideoTextureEmissive:xt&&M.emissiveMap.isVideoTexture===!0&&rt.getTransfer(M.emissiveMap.colorSpace)===ht,premultipliedAlpha:M.premultipliedAlpha,doubleSided:M.side===Vi,flipSided:M.side===_n,useDepthPacking:M.depthPacking>=0,depthPacking:M.depthPacking||0,index0AttributeName:M.index0AttributeName,extensionClipCullDistance:ke&&M.extensions.clipCullDistance===!0&&i.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(ke&&M.extensions.multiDraw===!0||$e)&&i.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:i.has("KHR_parallel_shader_compile"),customProgramCacheKey:M.customProgramCacheKey()};return Zt.vertexUv1s=l.has(1),Zt.vertexUv2s=l.has(2),Zt.vertexUv3s=l.has(3),l.clear(),Zt}function p(M){let x=[];if(M.shaderID?x.push(M.shaderID):(x.push(M.customVertexShaderID),x.push(M.customFragmentShaderID)),M.defines!==void 0)for(let D in M.defines)x.push(D),x.push(M.defines[D]);return M.isRawShaderMaterial===!1&&(S(x,M),E(x,M),x.push(n.outputColorSpace)),x.push(M.customProgramCacheKey),x.join()}function S(M,x){M.push(x.precision),M.push(x.outputColorSpace),M.push(x.envMapMode),M.push(x.envMapCubeUVHeight),M.push(x.mapUv),M.push(x.alphaMapUv),M.push(x.lightMapUv),M.push(x.aoMapUv),M.push(x.bumpMapUv),M.push(x.normalMapUv),M.push(x.displacementMapUv),M.push(x.emissiveMapUv),M.push(x.metalnessMapUv),M.push(x.roughnessMapUv),M.push(x.anisotropyMapUv),M.push(x.clearcoatMapUv),M.push(x.clearcoatNormalMapUv),M.push(x.clearcoatRoughnessMapUv),M.push(x.iridescenceMapUv),M.push(x.iridescenceThicknessMapUv),M.push(x.sheenColorMapUv),M.push(x.sheenRoughnessMapUv),M.push(x.specularMapUv),M.push(x.specularColorMapUv),M.push(x.specularIntensityMapUv),M.push(x.transmissionMapUv),M.push(x.thicknessMapUv),M.push(x.combine),M.push(x.fogExp2),M.push(x.sizeAttenuation),M.push(x.morphTargetsCount),M.push(x.morphAttributeCount),M.push(x.numDirLights),M.push(x.numPointLights),M.push(x.numSpotLights),M.push(x.numSpotLightMaps),M.push(x.numHemiLights),M.push(x.numRectAreaLights),M.push(x.numDirLightShadows),M.push(x.numPointLightShadows),M.push(x.numSpotLightShadows),M.push(x.numSpotLightShadowsWithMaps),M.push(x.numLightProbes),M.push(x.shadowMapType),M.push(x.toneMapping),M.push(x.numClippingPlanes),M.push(x.numClipIntersection),M.push(x.depthPacking)}function E(M,x){a.disableAll(),x.supportsVertexTextures&&a.enable(0),x.instancing&&a.enable(1),x.instancingColor&&a.enable(2),x.instancingMorph&&a.enable(3),x.matcap&&a.enable(4),x.envMap&&a.enable(5),x.normalMapObjectSpace&&a.enable(6),x.normalMapTangentSpace&&a.enable(7),x.clearcoat&&a.enable(8),x.iridescence&&a.enable(9),x.alphaTest&&a.enable(10),x.vertexColors&&a.enable(11),x.vertexAlphas&&a.enable(12),x.vertexUv1s&&a.enable(13),x.vertexUv2s&&a.enable(14),x.vertexUv3s&&a.enable(15),x.vertexTangents&&a.enable(16),x.anisotropy&&a.enable(17),x.alphaHash&&a.enable(18),x.batching&&a.enable(19),x.dispersion&&a.enable(20),x.batchingColor&&a.enable(21),M.push(a.mask),a.disableAll(),x.fog&&a.enable(0),x.useFog&&a.enable(1),x.flatShading&&a.enable(2),x.logarithmicDepthBuffer&&a.enable(3),x.reverseDepthBuffer&&a.enable(4),x.skinning&&a.enable(5),x.morphTargets&&a.enable(6),x.morphNormals&&a.enable(7),x.morphColors&&a.enable(8),x.premultipliedAlpha&&a.enable(9),x.shadowMapEnabled&&a.enable(10),x.doubleSided&&a.enable(11),x.flipSided&&a.enable(12),x.useDepthPacking&&a.enable(13),x.dithering&&a.enable(14),x.transmission&&a.enable(15),x.sheen&&a.enable(16),x.opaque&&a.enable(17),x.pointsUvs&&a.enable(18),x.decodeVideoTexture&&a.enable(19),x.decodeVideoTextureEmissive&&a.enable(20),x.alphaToCoverage&&a.enable(21),M.push(a.mask)}function b(M){let x=g[M.type],D;if(x){let W=oi[x];D=XD.clone(W.uniforms)}else D=M.uniforms;return D}function F(M,x){let D;for(let W=0,B=u.length;W<B;W++){let Z=u[W];if(Z.cacheKey===x){D=Z,++D.usedTimes;break}}return D===void 0&&(D=new nN(n,x,M,s),u.push(D)),D}function T(M){if(--M.usedTimes===0){let x=u.indexOf(M);u[x]=u[u.length-1],u.pop(),M.destroy()}}function C(M){c.remove(M)}function I(){c.dispose()}return{getParameters:m,getProgramCacheKey:p,getUniforms:b,acquireProgram:F,releaseProgram:T,releaseShaderCache:C,programs:u,dispose:I}}function sN(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function oN(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.z!==e.z?n.z-e.z:n.id-e.id}function q0(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function X0(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d,h,f,g,v,m){let p=n[e];return p===void 0?(p={id:d.id,object:d,geometry:h,material:f,groupOrder:g,renderOrder:d.renderOrder,z:v,group:m},n[e]=p):(p.id=d.id,p.object=d,p.geometry=h,p.material=f,p.groupOrder=g,p.renderOrder=d.renderOrder,p.z=v,p.group=m),e++,p}function a(d,h,f,g,v,m){let p=o(d,h,f,g,v,m);f.transmission>0?i.push(p):f.transparent===!0?r.push(p):t.push(p)}function c(d,h,f,g,v,m){let p=o(d,h,f,g,v,m);f.transmission>0?i.unshift(p):f.transparent===!0?r.unshift(p):t.unshift(p)}function l(d,h){t.length>1&&t.sort(d||oN),i.length>1&&i.sort(h||q0),r.length>1&&r.sort(h||q0)}function u(){for(let d=e,h=n.length;d<h;d++){let f=n[d];if(f.id===null)break;f.id=null,f.object=null,f.geometry=null,f.material=null,f.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:a,unshift:c,finish:u,sort:l}}function aN(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new X0,n.set(i,[o])):r>=s.length?(o=new X0,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function cN(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new O,color:new at};break;case"SpotLight":t={position:new O,direction:new O,color:new at,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new O,color:new at,distance:0,decay:0};break;case"HemisphereLight":t={direction:new O,skyColor:new at,groundColor:new at};break;case"RectAreaLight":t={color:new at,position:new O,halfWidth:new O,halfHeight:new O};break}return n[e.id]=t,t}}}function lN(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new st};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new st};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new st,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var uN=0;function dN(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function fN(n){let e=new cN,t=lN(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new O);let r=new O,s=new Tt,o=new Tt;function a(l){let u=0,d=0,h=0;for(let M=0;M<9;M++)i.probe[M].set(0,0,0);let f=0,g=0,v=0,m=0,p=0,S=0,E=0,b=0,F=0,T=0,C=0;l.sort(dN);for(let M=0,x=l.length;M<x;M++){let D=l[M],W=D.color,B=D.intensity,Z=D.distance,J=D.shadow&&D.shadow.map?D.shadow.map.texture:null;if(D.isAmbientLight)u+=W.r*B,d+=W.g*B,h+=W.b*B;else if(D.isLightProbe){for(let q=0;q<9;q++)i.probe[q].addScaledVector(D.sh.coefficients[q],B);C++}else if(D.isDirectionalLight){let q=e.get(D);if(q.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let Q=D.shadow,z=t.get(D);z.shadowIntensity=Q.intensity,z.shadowBias=Q.bias,z.shadowNormalBias=Q.normalBias,z.shadowRadius=Q.radius,z.shadowMapSize=Q.mapSize,i.directionalShadow[f]=z,i.directionalShadowMap[f]=J,i.directionalShadowMatrix[f]=D.shadow.matrix,S++}i.directional[f]=q,f++}else if(D.isSpotLight){let q=e.get(D);q.position.setFromMatrixPosition(D.matrixWorld),q.color.copy(W).multiplyScalar(B),q.distance=Z,q.coneCos=Math.cos(D.angle),q.penumbraCos=Math.cos(D.angle*(1-D.penumbra)),q.decay=D.decay,i.spot[v]=q;let Q=D.shadow;if(D.map&&(i.spotLightMap[F]=D.map,F++,Q.updateMatrices(D),D.castShadow&&T++),i.spotLightMatrix[v]=Q.matrix,D.castShadow){let z=t.get(D);z.shadowIntensity=Q.intensity,z.shadowBias=Q.bias,z.shadowNormalBias=Q.normalBias,z.shadowRadius=Q.radius,z.shadowMapSize=Q.mapSize,i.spotShadow[v]=z,i.spotShadowMap[v]=J,b++}v++}else if(D.isRectAreaLight){let q=e.get(D);q.color.copy(W).multiplyScalar(B),q.halfWidth.set(D.width*.5,0,0),q.halfHeight.set(0,D.height*.5,0),i.rectArea[m]=q,m++}else if(D.isPointLight){let q=e.get(D);if(q.color.copy(D.color).multiplyScalar(D.intensity),q.distance=D.distance,q.decay=D.decay,D.castShadow){let Q=D.shadow,z=t.get(D);z.shadowIntensity=Q.intensity,z.shadowBias=Q.bias,z.shadowNormalBias=Q.normalBias,z.shadowRadius=Q.radius,z.shadowMapSize=Q.mapSize,z.shadowCameraNear=Q.camera.near,z.shadowCameraFar=Q.camera.far,i.pointShadow[g]=z,i.pointShadowMap[g]=J,i.pointShadowMatrix[g]=D.shadow.matrix,E++}i.point[g]=q,g++}else if(D.isHemisphereLight){let q=e.get(D);q.skyColor.copy(D.color).multiplyScalar(B),q.groundColor.copy(D.groundColor).multiplyScalar(B),i.hemi[p]=q,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=se.LTC_FLOAT_1,i.rectAreaLTC2=se.LTC_FLOAT_2):(i.rectAreaLTC1=se.LTC_HALF_1,i.rectAreaLTC2=se.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=h;let I=i.hash;(I.directionalLength!==f||I.pointLength!==g||I.spotLength!==v||I.rectAreaLength!==m||I.hemiLength!==p||I.numDirectionalShadows!==S||I.numPointShadows!==E||I.numSpotShadows!==b||I.numSpotMaps!==F||I.numLightProbes!==C)&&(i.directional.length=f,i.spot.length=v,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=S,i.directionalShadowMap.length=S,i.pointShadow.length=E,i.pointShadowMap.length=E,i.spotShadow.length=b,i.spotShadowMap.length=b,i.directionalShadowMatrix.length=S,i.pointShadowMatrix.length=E,i.spotLightMatrix.length=b+F-T,i.spotLightMap.length=F,i.numSpotLightShadowsWithMaps=T,i.numLightProbes=C,I.directionalLength=f,I.pointLength=g,I.spotLength=v,I.rectAreaLength=m,I.hemiLength=p,I.numDirectionalShadows=S,I.numPointShadows=E,I.numSpotShadows=b,I.numSpotMaps=F,I.numLightProbes=C,i.version=uN++)}function c(l,u){let d=0,h=0,f=0,g=0,v=0,m=u.matrixWorldInverse;for(let p=0,S=l.length;p<S;p++){let E=l[p];if(E.isDirectionalLight){let b=i.directional[d];b.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),b.direction.sub(r),b.direction.transformDirection(m),d++}else if(E.isSpotLight){let b=i.spot[f];b.position.setFromMatrixPosition(E.matrixWorld),b.position.applyMatrix4(m),b.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),b.direction.sub(r),b.direction.transformDirection(m),f++}else if(E.isRectAreaLight){let b=i.rectArea[g];b.position.setFromMatrixPosition(E.matrixWorld),b.position.applyMatrix4(m),o.identity(),s.copy(E.matrixWorld),s.premultiply(m),o.extractRotation(s),b.halfWidth.set(E.width*.5,0,0),b.halfHeight.set(0,E.height*.5,0),b.halfWidth.applyMatrix4(o),b.halfHeight.applyMatrix4(o),g++}else if(E.isPointLight){let b=i.point[h];b.position.setFromMatrixPosition(E.matrixWorld),b.position.applyMatrix4(m),h++}else if(E.isHemisphereLight){let b=i.hemi[v];b.direction.setFromMatrixPosition(E.matrixWorld),b.direction.transformDirection(m),v++}}}return{setup:a,setupView:c,state:i}}function Y0(n){let e=new fN(n),t=[],i=[];function r(u){l.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function c(u){e.setupView(t,u)}let l={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:l,setupLights:a,setupLightsView:c,pushLight:s,pushShadow:o}}function hN(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new Y0(n),e.set(r,[a])):s>=o.length?(a=new Y0(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var Zp=class extends ss{static get type(){return"MeshDepthMaterial"}constructor(e){super(),this.isMeshDepthMaterial=!0,this.depthPacking=_D,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Jp=class extends ss{static get type(){return"MeshDistanceMaterial"}constructor(e){super(),this.isMeshDistanceMaterial=!0,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}},pN=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,mN=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
#include <packing>
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = unpackRGBATo2Half( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ) );
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = unpackRGBAToDepth( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ) );
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( squared_mean - mean * mean );
	gl_FragColor = pack2HalfToRGBA( vec2( mean, std_dev ) );
}`;function gN(n,e,t){let i=new xa,r=new st,s=new st,o=new pt,a=new Zp({depthPacking:xD}),c=new Jp,l={},u=t.maxTextureSize,d={[fr]:_n,[_n]:fr,[Vi]:Vi},h=new li({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new st},radius:{value:4}},vertexShader:pN,fragmentShader:mN}),f=h.clone();f.defines.HORIZONTAL_PASS=1;let g=new wn;g.setAttribute("position",new xn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let v=new Yt(g,h),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=ix;let p=this.type;this.render=function(T,C,I){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||T.length===0)return;let M=n.getRenderTarget(),x=n.getActiveCubeFace(),D=n.getActiveMipmapLevel(),W=n.state;W.setBlending(ur),W.buffers.color.setClear(1,1,1,1),W.buffers.depth.setTest(!0),W.setScissorTest(!1);let B=p!==ki&&this.type===ki,Z=p===ki&&this.type!==ki;for(let J=0,q=T.length;J<q;J++){let Q=T[J],z=Q.shadow;if(z===void 0){console.warn("THREE.WebGLShadowMap:",Q,"has no shadow.");continue}if(z.autoUpdate===!1&&z.needsUpdate===!1)continue;r.copy(z.mapSize);let ae=z.getFrameExtents();if(r.multiply(ae),s.copy(z.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/ae.x),r.x=s.x*ae.x,z.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/ae.y),r.y=s.y*ae.y,z.mapSize.y=s.y)),z.map===null||B===!0||Z===!0){let we=this.type!==ki?{minFilter:Zn,magFilter:Zn}:{};z.map!==null&&z.map.dispose(),z.map=new Wi(r.x,r.y,we),z.map.texture.name=Q.name+".shadowMap",z.camera.updateProjectionMatrix()}n.setRenderTarget(z.map),n.clear();let he=z.getViewportCount();for(let we=0;we<he;we++){let je=z.getViewport(we);o.set(s.x*je.x,s.y*je.y,s.x*je.z,s.y*je.w),W.viewport(o),z.updateMatrices(Q,we),i=z.getFrustum(),b(C,I,z.camera,Q,this.type)}z.isPointLightShadow!==!0&&this.type===ki&&S(z,I),z.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(M,x,D)};function S(T,C){let I=e.update(v);h.defines.VSM_SAMPLES!==T.blurSamples&&(h.defines.VSM_SAMPLES=T.blurSamples,f.defines.VSM_SAMPLES=T.blurSamples,h.needsUpdate=!0,f.needsUpdate=!0),T.mapPass===null&&(T.mapPass=new Wi(r.x,r.y)),h.uniforms.shadow_pass.value=T.map.texture,h.uniforms.resolution.value=T.mapSize,h.uniforms.radius.value=T.radius,n.setRenderTarget(T.mapPass),n.clear(),n.renderBufferDirect(C,null,I,h,v,null),f.uniforms.shadow_pass.value=T.mapPass.texture,f.uniforms.resolution.value=T.mapSize,f.uniforms.radius.value=T.radius,n.setRenderTarget(T.map),n.clear(),n.renderBufferDirect(C,null,I,f,v,null)}function E(T,C,I,M){let x=null,D=I.isPointLight===!0?T.customDistanceMaterial:T.customDepthMaterial;if(D!==void 0)x=D;else if(x=I.isPointLight===!0?c:a,n.localClippingEnabled&&C.clipShadows===!0&&Array.isArray(C.clippingPlanes)&&C.clippingPlanes.length!==0||C.displacementMap&&C.displacementScale!==0||C.alphaMap&&C.alphaTest>0||C.map&&C.alphaTest>0){let W=x.uuid,B=C.uuid,Z=l[W];Z===void 0&&(Z={},l[W]=Z);let J=Z[B];J===void 0&&(J=x.clone(),Z[B]=J,C.addEventListener("dispose",F)),x=J}if(x.visible=C.visible,x.wireframe=C.wireframe,M===ki?x.side=C.shadowSide!==null?C.shadowSide:C.side:x.side=C.shadowSide!==null?C.shadowSide:d[C.side],x.alphaMap=C.alphaMap,x.alphaTest=C.alphaTest,x.map=C.map,x.clipShadows=C.clipShadows,x.clippingPlanes=C.clippingPlanes,x.clipIntersection=C.clipIntersection,x.displacementMap=C.displacementMap,x.displacementScale=C.displacementScale,x.displacementBias=C.displacementBias,x.wireframeLinewidth=C.wireframeLinewidth,x.linewidth=C.linewidth,I.isPointLight===!0&&x.isMeshDistanceMaterial===!0){let W=n.properties.get(x);W.light=I}return x}function b(T,C,I,M,x){if(T.visible===!1)return;if(T.layers.test(C.layers)&&(T.isMesh||T.isLine||T.isPoints)&&(T.castShadow||T.receiveShadow&&x===ki)&&(!T.frustumCulled||i.intersectsObject(T))){T.modelViewMatrix.multiplyMatrices(I.matrixWorldInverse,T.matrixWorld);let B=e.update(T),Z=T.material;if(Array.isArray(Z)){let J=B.groups;for(let q=0,Q=J.length;q<Q;q++){let z=J[q],ae=Z[z.materialIndex];if(ae&&ae.visible){let he=E(T,ae,M,x);T.onBeforeShadow(n,T,C,I,B,he,z),n.renderBufferDirect(I,null,B,he,T,z),T.onAfterShadow(n,T,C,I,B,he,z)}}}else if(Z.visible){let J=E(T,Z,M,x);T.onBeforeShadow(n,T,C,I,B,J,null),n.renderBufferDirect(I,null,B,J,T,null),T.onAfterShadow(n,T,C,I,B,J,null)}}let W=T.children;for(let B=0,Z=W.length;B<Z;B++)b(W[B],C,I,M,x)}function F(T){T.target.removeEventListener("dispose",F);for(let I in l){let M=l[I],x=T.target.uuid;x in M&&(M[x].dispose(),delete M[x])}}}var vN={[Qh]:ep,[tp]:rp,[np]:sp,[to]:ip,[ep]:Qh,[rp]:tp,[sp]:np,[ip]:to};function yN(n,e){function t(){let A=!1,oe=new pt,H=null,Y=new pt(0,0,0,0);return{setMask:function(fe){H!==fe&&!A&&(n.colorMask(fe,fe,fe,fe),H=fe)},setLocked:function(fe){A=fe},setClear:function(fe,ue,ke,Dt,Zt){Zt===!0&&(fe*=Dt,ue*=Dt,ke*=Dt),oe.set(fe,ue,ke,Dt),Y.equals(oe)===!1&&(n.clearColor(fe,ue,ke,Dt),Y.copy(oe))},reset:function(){A=!1,H=null,Y.set(-1,0,0,0)}}}function i(){let A=!1,oe=!1,H=null,Y=null,fe=null;return{setReversed:function(ue){if(oe!==ue){let ke=e.get("EXT_clip_control");oe?ke.clipControlEXT(ke.LOWER_LEFT_EXT,ke.ZERO_TO_ONE_EXT):ke.clipControlEXT(ke.LOWER_LEFT_EXT,ke.NEGATIVE_ONE_TO_ONE_EXT);let Dt=fe;fe=null,this.setClear(Dt)}oe=ue},getReversed:function(){return oe},setTest:function(ue){ue?ce(n.DEPTH_TEST):Re(n.DEPTH_TEST)},setMask:function(ue){H!==ue&&!A&&(n.depthMask(ue),H=ue)},setFunc:function(ue){if(oe&&(ue=vN[ue]),Y!==ue){switch(ue){case Qh:n.depthFunc(n.NEVER);break;case ep:n.depthFunc(n.ALWAYS);break;case tp:n.depthFunc(n.LESS);break;case to:n.depthFunc(n.LEQUAL);break;case np:n.depthFunc(n.EQUAL);break;case ip:n.depthFunc(n.GEQUAL);break;case rp:n.depthFunc(n.GREATER);break;case sp:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}Y=ue}},setLocked:function(ue){A=ue},setClear:function(ue){fe!==ue&&(oe&&(ue=1-ue),n.clearDepth(ue),fe=ue)},reset:function(){A=!1,H=null,Y=null,fe=null,oe=!1}}}function r(){let A=!1,oe=null,H=null,Y=null,fe=null,ue=null,ke=null,Dt=null,Zt=null;return{setTest:function(ut){A||(ut?ce(n.STENCIL_TEST):Re(n.STENCIL_TEST))},setMask:function(ut){oe!==ut&&!A&&(n.stencilMask(ut),oe=ut)},setFunc:function(ut,Vn,di){(H!==ut||Y!==Vn||fe!==di)&&(n.stencilFunc(ut,Vn,di),H=ut,Y=Vn,fe=di)},setOp:function(ut,Vn,di){(ue!==ut||ke!==Vn||Dt!==di)&&(n.stencilOp(ut,Vn,di),ue=ut,ke=Vn,Dt=di)},setLocked:function(ut){A=ut},setClear:function(ut){Zt!==ut&&(n.clearStencil(ut),Zt=ut)},reset:function(){A=!1,oe=null,H=null,Y=null,fe=null,ue=null,ke=null,Dt=null,Zt=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},h=new WeakMap,f=[],g=null,v=!1,m=null,p=null,S=null,E=null,b=null,F=null,T=null,C=new at(0,0,0),I=0,M=!1,x=null,D=null,W=null,B=null,Z=null,J=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),q=!1,Q=0,z=n.getParameter(n.VERSION);z.indexOf("WebGL")!==-1?(Q=parseFloat(/^WebGL (\d)/.exec(z)[1]),q=Q>=1):z.indexOf("OpenGL ES")!==-1&&(Q=parseFloat(/^OpenGL ES (\d)/.exec(z)[1]),q=Q>=2);let ae=null,he={},we=n.getParameter(n.SCISSOR_BOX),je=n.getParameter(n.VIEWPORT),mt=new pt().fromArray(we),$=new pt().fromArray(je);function re(A,oe,H,Y){let fe=new Uint8Array(4),ue=n.createTexture();n.bindTexture(A,ue),n.texParameteri(A,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(A,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let ke=0;ke<H;ke++)A===n.TEXTURE_3D||A===n.TEXTURE_2D_ARRAY?n.texImage3D(oe,0,n.RGBA,1,1,Y,0,n.RGBA,n.UNSIGNED_BYTE,fe):n.texImage2D(oe+ke,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,fe);return ue}let be={};be[n.TEXTURE_2D]=re(n.TEXTURE_2D,n.TEXTURE_2D,1),be[n.TEXTURE_CUBE_MAP]=re(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),be[n.TEXTURE_2D_ARRAY]=re(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),be[n.TEXTURE_3D]=re(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ce(n.DEPTH_TEST),o.setFunc(to),Ke(!1),Qe(Z_),ce(n.CULL_FACE),P(ur);function ce(A){u[A]!==!0&&(n.enable(A),u[A]=!0)}function Re(A){u[A]!==!1&&(n.disable(A),u[A]=!1)}function Le(A,oe){return d[A]!==oe?(n.bindFramebuffer(A,oe),d[A]=oe,A===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=oe),A===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=oe),!0):!1}function $e(A,oe){let H=f,Y=!1;if(A){H=h.get(oe),H===void 0&&(H=[],h.set(oe,H));let fe=A.textures;if(H.length!==fe.length||H[0]!==n.COLOR_ATTACHMENT0){for(let ue=0,ke=fe.length;ue<ke;ue++)H[ue]=n.COLOR_ATTACHMENT0+ue;H.length=fe.length,Y=!0}}else H[0]!==n.BACK&&(H[0]=n.BACK,Y=!0);Y&&n.drawBuffers(H)}function St(A){return g!==A?(n.useProgram(A),g=A,!0):!1}let nt={[Kr]:n.FUNC_ADD,[$T]:n.FUNC_SUBTRACT,[qT]:n.FUNC_REVERSE_SUBTRACT};nt[XT]=n.MIN,nt[YT]=n.MAX;let Nt={[ZT]:n.ZERO,[JT]:n.ONE,[KT]:n.SRC_COLOR,[Jh]:n.SRC_ALPHA,[rD]:n.SRC_ALPHA_SATURATE,[nD]:n.DST_COLOR,[eD]:n.DST_ALPHA,[QT]:n.ONE_MINUS_SRC_COLOR,[Kh]:n.ONE_MINUS_SRC_ALPHA,[iD]:n.ONE_MINUS_DST_COLOR,[tD]:n.ONE_MINUS_DST_ALPHA,[sD]:n.CONSTANT_COLOR,[oD]:n.ONE_MINUS_CONSTANT_COLOR,[aD]:n.CONSTANT_ALPHA,[cD]:n.ONE_MINUS_CONSTANT_ALPHA};function P(A,oe,H,Y,fe,ue,ke,Dt,Zt,ut){if(A===ur){v===!0&&(Re(n.BLEND),v=!1);return}if(v===!1&&(ce(n.BLEND),v=!0),A!==jT){if(A!==m||ut!==M){if((p!==Kr||b!==Kr)&&(n.blendEquation(n.FUNC_ADD),p=Kr,b=Kr),ut)switch(A){case Js:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case J_:n.blendFunc(n.ONE,n.ONE);break;case K_:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Q_:n.blendFuncSeparate(n.ZERO,n.SRC_COLOR,n.ZERO,n.SRC_ALPHA);break;default:console.error("THREE.WebGLState: Invalid blending: ",A);break}else switch(A){case Js:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case J_:n.blendFunc(n.SRC_ALPHA,n.ONE);break;case K_:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Q_:n.blendFunc(n.ZERO,n.SRC_COLOR);break;default:console.error("THREE.WebGLState: Invalid blending: ",A);break}S=null,E=null,F=null,T=null,C.set(0,0,0),I=0,m=A,M=ut}return}fe=fe||oe,ue=ue||H,ke=ke||Y,(oe!==p||fe!==b)&&(n.blendEquationSeparate(nt[oe],nt[fe]),p=oe,b=fe),(H!==S||Y!==E||ue!==F||ke!==T)&&(n.blendFuncSeparate(Nt[H],Nt[Y],Nt[ue],Nt[ke]),S=H,E=Y,F=ue,T=ke),(Dt.equals(C)===!1||Zt!==I)&&(n.blendColor(Dt.r,Dt.g,Dt.b,Zt),C.copy(Dt),I=Zt),m=A,M=!1}function Cn(A,oe){A.side===Vi?Re(n.CULL_FACE):ce(n.CULL_FACE);let H=A.side===_n;oe&&(H=!H),Ke(H),A.blending===Js&&A.transparent===!1?P(ur):P(A.blending,A.blendEquation,A.blendSrc,A.blendDst,A.blendEquationAlpha,A.blendSrcAlpha,A.blendDstAlpha,A.blendColor,A.blendAlpha,A.premultipliedAlpha),o.setFunc(A.depthFunc),o.setTest(A.depthTest),o.setMask(A.depthWrite),s.setMask(A.colorWrite);let Y=A.stencilWrite;a.setTest(Y),Y&&(a.setMask(A.stencilWriteMask),a.setFunc(A.stencilFunc,A.stencilRef,A.stencilFuncMask),a.setOp(A.stencilFail,A.stencilZFail,A.stencilZPass)),xt(A.polygonOffset,A.polygonOffsetFactor,A.polygonOffsetUnits),A.alphaToCoverage===!0?ce(n.SAMPLE_ALPHA_TO_COVERAGE):Re(n.SAMPLE_ALPHA_TO_COVERAGE)}function Ke(A){x!==A&&(A?n.frontFace(n.CW):n.frontFace(n.CCW),x=A)}function Qe(A){A!==zT?(ce(n.CULL_FACE),A!==D&&(A===Z_?n.cullFace(n.BACK):A===GT?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Re(n.CULL_FACE),D=A}function De(A){A!==W&&(q&&n.lineWidth(A),W=A)}function xt(A,oe,H){A?(ce(n.POLYGON_OFFSET_FILL),(B!==oe||Z!==H)&&(n.polygonOffset(oe,H),B=oe,Z=H)):Re(n.POLYGON_OFFSET_FILL)}function Ce(A){A?ce(n.SCISSOR_TEST):Re(n.SCISSOR_TEST)}function w(A){A===void 0&&(A=n.TEXTURE0+J-1),ae!==A&&(n.activeTexture(A),ae=A)}function y(A,oe,H){H===void 0&&(ae===null?H=n.TEXTURE0+J-1:H=ae);let Y=he[H];Y===void 0&&(Y={type:void 0,texture:void 0},he[H]=Y),(Y.type!==A||Y.texture!==oe)&&(ae!==H&&(n.activeTexture(H),ae=H),n.bindTexture(A,oe||be[A]),Y.type=A,Y.texture=oe)}function L(){let A=he[ae];A!==void 0&&A.type!==void 0&&(n.bindTexture(A.type,null),A.type=void 0,A.texture=void 0)}function X(){try{n.compressedTexImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function K(){try{n.compressedTexImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function j(){try{n.texSubImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Me(){try{n.texSubImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function le(){try{n.compressedTexSubImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function pe(){try{n.compressedTexSubImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function it(){try{n.texStorage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function te(){try{n.texStorage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function me(){try{n.texImage2D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Ae(){try{n.texImage3D.apply(n,arguments)}catch(A){console.error("THREE.WebGLState:",A)}}function Pe(A){mt.equals(A)===!1&&(n.scissor(A.x,A.y,A.z,A.w),mt.copy(A))}function ge(A){$.equals(A)===!1&&(n.viewport(A.x,A.y,A.z,A.w),$.copy(A))}function et(A,oe){let H=l.get(oe);H===void 0&&(H=new WeakMap,l.set(oe,H));let Y=H.get(A);Y===void 0&&(Y=n.getUniformBlockIndex(oe,A.name),H.set(A,Y))}function Be(A,oe){let Y=l.get(oe).get(A);c.get(oe)!==Y&&(n.uniformBlockBinding(oe,Y,A.__bindingPointIndex),c.set(oe,Y))}function gt(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},ae=null,he={},d={},h=new WeakMap,f=[],g=null,v=!1,m=null,p=null,S=null,E=null,b=null,F=null,T=null,C=new at(0,0,0),I=0,M=!1,x=null,D=null,W=null,B=null,Z=null,mt.set(0,0,n.canvas.width,n.canvas.height),$.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ce,disable:Re,bindFramebuffer:Le,drawBuffers:$e,useProgram:St,setBlending:P,setMaterial:Cn,setFlipSided:Ke,setCullFace:Qe,setLineWidth:De,setPolygonOffset:xt,setScissorTest:Ce,activeTexture:w,bindTexture:y,unbindTexture:L,compressedTexImage2D:X,compressedTexImage3D:K,texImage2D:me,texImage3D:Ae,updateUBOMapping:et,uniformBlockBinding:Be,texStorage2D:it,texStorage3D:te,texSubImage2D:j,texSubImage3D:Me,compressedTexSubImage2D:le,compressedTexSubImage3D:pe,scissor:Pe,viewport:ge,reset:gt}}function Z0(n,e,t,i){let r=_N(i);switch(t){case cx:return n*e;case ux:return n*e;case dx:return n*e*2;case fx:return n*e/r.components*r.byteLength;case gm:return n*e/r.components*r.byteLength;case hx:return n*e*2/r.components*r.byteLength;case vm:return n*e*2/r.components*r.byteLength;case lx:return n*e*3/r.components*r.byteLength;case Yn:return n*e*4/r.components*r.byteLength;case ym:return n*e*4/r.components*r.byteLength;case Fl:case Ll:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case kl:case Ul:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case dp:case hp:return Math.max(n,16)*Math.max(e,8)/4;case up:case fp:return Math.max(n,8)*Math.max(e,8)/2;case pp:case mp:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case gp:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case vp:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case yp:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case _p:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case xp:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case bp:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Mp:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Ep:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Sp:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case wp:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Cp:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Tp:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Dp:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Ap:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Ip:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Vl:case Rp:case Np:return Math.ceil(n/4)*Math.ceil(e/4)*16;case px:case Pp:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Op:case Fp:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function _N(n){switch(n){case Gi:case sx:return{byteLength:1,components:1};case ya:case ox:case Ma:return{byteLength:2,components:1};case pm:case mm:return{byteLength:2,components:4};case is:case hm:case Bi:return{byteLength:4,components:1};case ax:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}function xN(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new st,u=new WeakMap,d,h=new WeakMap,f=!1;try{f=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(w,y){return f?new OffscreenCanvas(w,y):zl("canvas")}function v(w,y,L){let X=1,K=Ce(w);if((K.width>L||K.height>L)&&(X=L/Math.max(K.width,K.height)),X<1)if(typeof HTMLImageElement<"u"&&w instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&w instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&w instanceof ImageBitmap||typeof VideoFrame<"u"&&w instanceof VideoFrame){let j=Math.floor(X*K.width),Me=Math.floor(X*K.height);d===void 0&&(d=g(j,Me));let le=y?g(j,Me):d;return le.width=j,le.height=Me,le.getContext("2d").drawImage(w,0,0,j,Me),console.warn("THREE.WebGLRenderer: Texture has been resized from ("+K.width+"x"+K.height+") to ("+j+"x"+Me+")."),le}else return"data"in w&&console.warn("THREE.WebGLRenderer: Image in DataTexture is too big ("+K.width+"x"+K.height+")."),w;return w}function m(w){return w.generateMipmaps}function p(w){n.generateMipmap(w)}function S(w){return w.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:w.isWebGL3DRenderTarget?n.TEXTURE_3D:w.isWebGLArrayRenderTarget||w.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function E(w,y,L,X,K=!1){if(w!==null){if(n[w]!==void 0)return n[w];console.warn("THREE.WebGLRenderer: Attempt to use non-existing WebGL internal format '"+w+"'")}let j=y;if(y===n.RED&&(L===n.FLOAT&&(j=n.R32F),L===n.HALF_FLOAT&&(j=n.R16F),L===n.UNSIGNED_BYTE&&(j=n.R8)),y===n.RED_INTEGER&&(L===n.UNSIGNED_BYTE&&(j=n.R8UI),L===n.UNSIGNED_SHORT&&(j=n.R16UI),L===n.UNSIGNED_INT&&(j=n.R32UI),L===n.BYTE&&(j=n.R8I),L===n.SHORT&&(j=n.R16I),L===n.INT&&(j=n.R32I)),y===n.RG&&(L===n.FLOAT&&(j=n.RG32F),L===n.HALF_FLOAT&&(j=n.RG16F),L===n.UNSIGNED_BYTE&&(j=n.RG8)),y===n.RG_INTEGER&&(L===n.UNSIGNED_BYTE&&(j=n.RG8UI),L===n.UNSIGNED_SHORT&&(j=n.RG16UI),L===n.UNSIGNED_INT&&(j=n.RG32UI),L===n.BYTE&&(j=n.RG8I),L===n.SHORT&&(j=n.RG16I),L===n.INT&&(j=n.RG32I)),y===n.RGB_INTEGER&&(L===n.UNSIGNED_BYTE&&(j=n.RGB8UI),L===n.UNSIGNED_SHORT&&(j=n.RGB16UI),L===n.UNSIGNED_INT&&(j=n.RGB32UI),L===n.BYTE&&(j=n.RGB8I),L===n.SHORT&&(j=n.RGB16I),L===n.INT&&(j=n.RGB32I)),y===n.RGBA_INTEGER&&(L===n.UNSIGNED_BYTE&&(j=n.RGBA8UI),L===n.UNSIGNED_SHORT&&(j=n.RGBA16UI),L===n.UNSIGNED_INT&&(j=n.RGBA32UI),L===n.BYTE&&(j=n.RGBA8I),L===n.SHORT&&(j=n.RGBA16I),L===n.INT&&(j=n.RGBA32I)),y===n.RGB&&L===n.UNSIGNED_INT_5_9_9_9_REV&&(j=n.RGB9_E5),y===n.RGBA){let Me=K?du:rt.getTransfer(X);L===n.FLOAT&&(j=n.RGBA32F),L===n.HALF_FLOAT&&(j=n.RGBA16F),L===n.UNSIGNED_BYTE&&(j=Me===ht?n.SRGB8_ALPHA8:n.RGBA8),L===n.UNSIGNED_SHORT_4_4_4_4&&(j=n.RGBA4),L===n.UNSIGNED_SHORT_5_5_5_1&&(j=n.RGB5_A1)}return(j===n.R16F||j===n.R32F||j===n.RG16F||j===n.RG32F||j===n.RGBA16F||j===n.RGBA32F)&&e.get("EXT_color_buffer_float"),j}function b(w,y){let L;return w?y===null||y===is||y===ro?L=n.DEPTH24_STENCIL8:y===Bi?L=n.DEPTH32F_STENCIL8:y===ya&&(L=n.DEPTH24_STENCIL8,console.warn("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):y===null||y===is||y===ro?L=n.DEPTH_COMPONENT24:y===Bi?L=n.DEPTH_COMPONENT32F:y===ya&&(L=n.DEPTH_COMPONENT16),L}function F(w,y){return m(w)===!0||w.isFramebufferTexture&&w.minFilter!==Zn&&w.minFilter!==ai?Math.log2(Math.max(y.width,y.height))+1:w.mipmaps!==void 0&&w.mipmaps.length>0?w.mipmaps.length:w.isCompressedTexture&&Array.isArray(w.image)?y.mipmaps.length:1}function T(w){let y=w.target;y.removeEventListener("dispose",T),I(y),y.isVideoTexture&&u.delete(y)}function C(w){let y=w.target;y.removeEventListener("dispose",C),x(y)}function I(w){let y=i.get(w);if(y.__webglInit===void 0)return;let L=w.source,X=h.get(L);if(X){let K=X[y.__cacheKey];K.usedTimes--,K.usedTimes===0&&M(w),Object.keys(X).length===0&&h.delete(L)}i.remove(w)}function M(w){let y=i.get(w);n.deleteTexture(y.__webglTexture);let L=w.source,X=h.get(L);delete X[y.__cacheKey],o.memory.textures--}function x(w){let y=i.get(w);if(w.depthTexture&&(w.depthTexture.dispose(),i.remove(w.depthTexture)),w.isWebGLCubeRenderTarget)for(let X=0;X<6;X++){if(Array.isArray(y.__webglFramebuffer[X]))for(let K=0;K<y.__webglFramebuffer[X].length;K++)n.deleteFramebuffer(y.__webglFramebuffer[X][K]);else n.deleteFramebuffer(y.__webglFramebuffer[X]);y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer[X])}else{if(Array.isArray(y.__webglFramebuffer))for(let X=0;X<y.__webglFramebuffer.length;X++)n.deleteFramebuffer(y.__webglFramebuffer[X]);else n.deleteFramebuffer(y.__webglFramebuffer);if(y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer),y.__webglMultisampledFramebuffer&&n.deleteFramebuffer(y.__webglMultisampledFramebuffer),y.__webglColorRenderbuffer)for(let X=0;X<y.__webglColorRenderbuffer.length;X++)y.__webglColorRenderbuffer[X]&&n.deleteRenderbuffer(y.__webglColorRenderbuffer[X]);y.__webglDepthRenderbuffer&&n.deleteRenderbuffer(y.__webglDepthRenderbuffer)}let L=w.textures;for(let X=0,K=L.length;X<K;X++){let j=i.get(L[X]);j.__webglTexture&&(n.deleteTexture(j.__webglTexture),o.memory.textures--),i.remove(L[X])}i.remove(w)}let D=0;function W(){D=0}function B(){let w=D;return w>=r.maxTextures&&console.warn("THREE.WebGLTextures: Trying to use "+w+" texture units while this GPU supports only "+r.maxTextures),D+=1,w}function Z(w){let y=[];return y.push(w.wrapS),y.push(w.wrapT),y.push(w.wrapR||0),y.push(w.magFilter),y.push(w.minFilter),y.push(w.anisotropy),y.push(w.internalFormat),y.push(w.format),y.push(w.type),y.push(w.generateMipmaps),y.push(w.premultiplyAlpha),y.push(w.flipY),y.push(w.unpackAlignment),y.push(w.colorSpace),y.join()}function J(w,y){let L=i.get(w);if(w.isVideoTexture&&De(w),w.isRenderTargetTexture===!1&&w.version>0&&L.__version!==w.version){let X=w.image;if(X===null)console.warn("THREE.WebGLRenderer: Texture marked for update but no image data found.");else if(X.complete===!1)console.warn("THREE.WebGLRenderer: Texture marked for update but image is incomplete");else{$(L,w,y);return}}t.bindTexture(n.TEXTURE_2D,L.__webglTexture,n.TEXTURE0+y)}function q(w,y){let L=i.get(w);if(w.version>0&&L.__version!==w.version){$(L,w,y);return}t.bindTexture(n.TEXTURE_2D_ARRAY,L.__webglTexture,n.TEXTURE0+y)}function Q(w,y){let L=i.get(w);if(w.version>0&&L.__version!==w.version){$(L,w,y);return}t.bindTexture(n.TEXTURE_3D,L.__webglTexture,n.TEXTURE0+y)}function z(w,y){let L=i.get(w);if(w.version>0&&L.__version!==w.version){re(L,w,y);return}t.bindTexture(n.TEXTURE_CUBE_MAP,L.__webglTexture,n.TEXTURE0+y)}let ae={[cp]:n.REPEAT,[ts]:n.CLAMP_TO_EDGE,[lp]:n.MIRRORED_REPEAT},he={[Zn]:n.NEAREST,[yD]:n.NEAREST_MIPMAP_NEAREST,[dl]:n.NEAREST_MIPMAP_LINEAR,[ai]:n.LINEAR,[_h]:n.LINEAR_MIPMAP_NEAREST,[ns]:n.LINEAR_MIPMAP_LINEAR},we={[ED]:n.NEVER,[AD]:n.ALWAYS,[SD]:n.LESS,[mx]:n.LEQUAL,[wD]:n.EQUAL,[DD]:n.GEQUAL,[CD]:n.GREATER,[TD]:n.NOTEQUAL};function je(w,y){if(y.type===Bi&&e.has("OES_texture_float_linear")===!1&&(y.magFilter===ai||y.magFilter===_h||y.magFilter===dl||y.magFilter===ns||y.minFilter===ai||y.minFilter===_h||y.minFilter===dl||y.minFilter===ns)&&console.warn("THREE.WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(w,n.TEXTURE_WRAP_S,ae[y.wrapS]),n.texParameteri(w,n.TEXTURE_WRAP_T,ae[y.wrapT]),(w===n.TEXTURE_3D||w===n.TEXTURE_2D_ARRAY)&&n.texParameteri(w,n.TEXTURE_WRAP_R,ae[y.wrapR]),n.texParameteri(w,n.TEXTURE_MAG_FILTER,he[y.magFilter]),n.texParameteri(w,n.TEXTURE_MIN_FILTER,he[y.minFilter]),y.compareFunction&&(n.texParameteri(w,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(w,n.TEXTURE_COMPARE_FUNC,we[y.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(y.magFilter===Zn||y.minFilter!==dl&&y.minFilter!==ns||y.type===Bi&&e.has("OES_texture_float_linear")===!1)return;if(y.anisotropy>1||i.get(y).__currentAnisotropy){let L=e.get("EXT_texture_filter_anisotropic");n.texParameterf(w,L.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(y.anisotropy,r.getMaxAnisotropy())),i.get(y).__currentAnisotropy=y.anisotropy}}}function mt(w,y){let L=!1;w.__webglInit===void 0&&(w.__webglInit=!0,y.addEventListener("dispose",T));let X=y.source,K=h.get(X);K===void 0&&(K={},h.set(X,K));let j=Z(y);if(j!==w.__cacheKey){K[j]===void 0&&(K[j]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,L=!0),K[j].usedTimes++;let Me=K[w.__cacheKey];Me!==void 0&&(K[w.__cacheKey].usedTimes--,Me.usedTimes===0&&M(y)),w.__cacheKey=j,w.__webglTexture=K[j].texture}return L}function $(w,y,L){let X=n.TEXTURE_2D;(y.isDataArrayTexture||y.isCompressedArrayTexture)&&(X=n.TEXTURE_2D_ARRAY),y.isData3DTexture&&(X=n.TEXTURE_3D);let K=mt(w,y),j=y.source;t.bindTexture(X,w.__webglTexture,n.TEXTURE0+L);let Me=i.get(j);if(j.version!==Me.__version||K===!0){t.activeTexture(n.TEXTURE0+L);let le=rt.getPrimaries(rt.workingColorSpace),pe=y.colorSpace===lr?null:rt.getPrimaries(y.colorSpace),it=y.colorSpace===lr||le===pe?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,it);let te=v(y.image,!1,r.maxTextureSize);te=xt(y,te);let me=s.convert(y.format,y.colorSpace),Ae=s.convert(y.type),Pe=E(y.internalFormat,me,Ae,y.colorSpace,y.isVideoTexture);je(X,y);let ge,et=y.mipmaps,Be=y.isVideoTexture!==!0,gt=Me.__version===void 0||K===!0,A=j.dataReady,oe=F(y,te);if(y.isDepthTexture)Pe=b(y.format===so,y.type),gt&&(Be?t.texStorage2D(n.TEXTURE_2D,1,Pe,te.width,te.height):t.texImage2D(n.TEXTURE_2D,0,Pe,te.width,te.height,0,me,Ae,null));else if(y.isDataTexture)if(et.length>0){Be&&gt&&t.texStorage2D(n.TEXTURE_2D,oe,Pe,et[0].width,et[0].height);for(let H=0,Y=et.length;H<Y;H++)ge=et[H],Be?A&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,Ae,ge.data):t.texImage2D(n.TEXTURE_2D,H,Pe,ge.width,ge.height,0,me,Ae,ge.data);y.generateMipmaps=!1}else Be?(gt&&t.texStorage2D(n.TEXTURE_2D,oe,Pe,te.width,te.height),A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,te.width,te.height,me,Ae,te.data)):t.texImage2D(n.TEXTURE_2D,0,Pe,te.width,te.height,0,me,Ae,te.data);else if(y.isCompressedTexture)if(y.isCompressedArrayTexture){Be&&gt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,oe,Pe,et[0].width,et[0].height,te.depth);for(let H=0,Y=et.length;H<Y;H++)if(ge=et[H],y.format!==Yn)if(me!==null)if(Be){if(A)if(y.layerUpdates.size>0){let fe=Z0(ge.width,ge.height,y.format,y.type);for(let ue of y.layerUpdates){let ke=ge.data.subarray(ue*fe/ge.data.BYTES_PER_ELEMENT,(ue+1)*fe/ge.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,ue,ge.width,ge.height,1,me,ke)}y.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,ge.width,ge.height,te.depth,me,ge.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,H,Pe,ge.width,ge.height,te.depth,0,ge.data,0,0);else console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Be?A&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,H,0,0,0,ge.width,ge.height,te.depth,me,Ae,ge.data):t.texImage3D(n.TEXTURE_2D_ARRAY,H,Pe,ge.width,ge.height,te.depth,0,me,Ae,ge.data)}else{Be&&gt&&t.texStorage2D(n.TEXTURE_2D,oe,Pe,et[0].width,et[0].height);for(let H=0,Y=et.length;H<Y;H++)ge=et[H],y.format!==Yn?me!==null?Be?A&&t.compressedTexSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,ge.data):t.compressedTexImage2D(n.TEXTURE_2D,H,Pe,ge.width,ge.height,0,ge.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Be?A&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,ge.width,ge.height,me,Ae,ge.data):t.texImage2D(n.TEXTURE_2D,H,Pe,ge.width,ge.height,0,me,Ae,ge.data)}else if(y.isDataArrayTexture)if(Be){if(gt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,oe,Pe,te.width,te.height,te.depth),A)if(y.layerUpdates.size>0){let H=Z0(te.width,te.height,y.format,y.type);for(let Y of y.layerUpdates){let fe=te.data.subarray(Y*H/te.data.BYTES_PER_ELEMENT,(Y+1)*H/te.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,Y,te.width,te.height,1,me,Ae,fe)}y.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,te.width,te.height,te.depth,me,Ae,te.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Pe,te.width,te.height,te.depth,0,me,Ae,te.data);else if(y.isData3DTexture)Be?(gt&&t.texStorage3D(n.TEXTURE_3D,oe,Pe,te.width,te.height,te.depth),A&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,te.width,te.height,te.depth,me,Ae,te.data)):t.texImage3D(n.TEXTURE_3D,0,Pe,te.width,te.height,te.depth,0,me,Ae,te.data);else if(y.isFramebufferTexture){if(gt)if(Be)t.texStorage2D(n.TEXTURE_2D,oe,Pe,te.width,te.height);else{let H=te.width,Y=te.height;for(let fe=0;fe<oe;fe++)t.texImage2D(n.TEXTURE_2D,fe,Pe,H,Y,0,me,Ae,null),H>>=1,Y>>=1}}else if(et.length>0){if(Be&&gt){let H=Ce(et[0]);t.texStorage2D(n.TEXTURE_2D,oe,Pe,H.width,H.height)}for(let H=0,Y=et.length;H<Y;H++)ge=et[H],Be?A&&t.texSubImage2D(n.TEXTURE_2D,H,0,0,me,Ae,ge):t.texImage2D(n.TEXTURE_2D,H,Pe,me,Ae,ge);y.generateMipmaps=!1}else if(Be){if(gt){let H=Ce(te);t.texStorage2D(n.TEXTURE_2D,oe,Pe,H.width,H.height)}A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,me,Ae,te)}else t.texImage2D(n.TEXTURE_2D,0,Pe,me,Ae,te);m(y)&&p(X),Me.__version=j.version,y.onUpdate&&y.onUpdate(y)}w.__version=y.version}function re(w,y,L){if(y.image.length!==6)return;let X=mt(w,y),K=y.source;t.bindTexture(n.TEXTURE_CUBE_MAP,w.__webglTexture,n.TEXTURE0+L);let j=i.get(K);if(K.version!==j.__version||X===!0){t.activeTexture(n.TEXTURE0+L);let Me=rt.getPrimaries(rt.workingColorSpace),le=y.colorSpace===lr?null:rt.getPrimaries(y.colorSpace),pe=y.colorSpace===lr||Me===le?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,pe);let it=y.isCompressedTexture||y.image[0].isCompressedTexture,te=y.image[0]&&y.image[0].isDataTexture,me=[];for(let Y=0;Y<6;Y++)!it&&!te?me[Y]=v(y.image[Y],!0,r.maxCubemapSize):me[Y]=te?y.image[Y].image:y.image[Y],me[Y]=xt(y,me[Y]);let Ae=me[0],Pe=s.convert(y.format,y.colorSpace),ge=s.convert(y.type),et=E(y.internalFormat,Pe,ge,y.colorSpace),Be=y.isVideoTexture!==!0,gt=j.__version===void 0||X===!0,A=K.dataReady,oe=F(y,Ae);je(n.TEXTURE_CUBE_MAP,y);let H;if(it){Be&&gt&&t.texStorage2D(n.TEXTURE_CUBE_MAP,oe,et,Ae.width,Ae.height);for(let Y=0;Y<6;Y++){H=me[Y].mipmaps;for(let fe=0;fe<H.length;fe++){let ue=H[fe];y.format!==Yn?Pe!==null?Be?A&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe,0,0,ue.width,ue.height,Pe,ue.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe,et,ue.width,ue.height,0,ue.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):Be?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe,0,0,ue.width,ue.height,Pe,ge,ue.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe,et,ue.width,ue.height,0,Pe,ge,ue.data)}}}else{if(H=y.mipmaps,Be&&gt){H.length>0&&oe++;let Y=Ce(me[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,oe,et,Y.width,Y.height)}for(let Y=0;Y<6;Y++)if(te){Be?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,0,0,me[Y].width,me[Y].height,Pe,ge,me[Y].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,et,me[Y].width,me[Y].height,0,Pe,ge,me[Y].data);for(let fe=0;fe<H.length;fe++){let ke=H[fe].image[Y].image;Be?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe+1,0,0,ke.width,ke.height,Pe,ge,ke.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe+1,et,ke.width,ke.height,0,Pe,ge,ke.data)}}else{Be?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,0,0,Pe,ge,me[Y]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,0,et,Pe,ge,me[Y]);for(let fe=0;fe<H.length;fe++){let ue=H[fe];Be?A&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe+1,0,0,Pe,ge,ue.image[Y]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Y,fe+1,et,Pe,ge,ue.image[Y])}}}m(y)&&p(n.TEXTURE_CUBE_MAP),j.__version=K.version,y.onUpdate&&y.onUpdate(y)}w.__version=y.version}function be(w,y,L,X,K,j){let Me=s.convert(L.format,L.colorSpace),le=s.convert(L.type),pe=E(L.internalFormat,Me,le,L.colorSpace),it=i.get(y),te=i.get(L);if(te.__renderTarget=y,!it.__hasExternalTextures){let me=Math.max(1,y.width>>j),Ae=Math.max(1,y.height>>j);K===n.TEXTURE_3D||K===n.TEXTURE_2D_ARRAY?t.texImage3D(K,j,pe,me,Ae,y.depth,0,Me,le,null):t.texImage2D(K,j,pe,me,Ae,0,Me,le,null)}t.bindFramebuffer(n.FRAMEBUFFER,w),Qe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,X,K,te.__webglTexture,0,Ke(y)):(K===n.TEXTURE_2D||K>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&K<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,X,K,te.__webglTexture,j),t.bindFramebuffer(n.FRAMEBUFFER,null)}function ce(w,y,L){if(n.bindRenderbuffer(n.RENDERBUFFER,w),y.depthBuffer){let X=y.depthTexture,K=X&&X.isDepthTexture?X.type:null,j=b(y.stencilBuffer,K),Me=y.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,le=Ke(y);Qe(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,le,j,y.width,y.height):L?n.renderbufferStorageMultisample(n.RENDERBUFFER,le,j,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,j,y.width,y.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,Me,n.RENDERBUFFER,w)}else{let X=y.textures;for(let K=0;K<X.length;K++){let j=X[K],Me=s.convert(j.format,j.colorSpace),le=s.convert(j.type),pe=E(j.internalFormat,Me,le,j.colorSpace),it=Ke(y);L&&Qe(y)===!1?n.renderbufferStorageMultisample(n.RENDERBUFFER,it,pe,y.width,y.height):Qe(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,it,pe,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,pe,y.width,y.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Re(w,y){if(y&&y.isWebGLCubeRenderTarget)throw new Error("Depth Texture with cube render targets is not supported");if(t.bindFramebuffer(n.FRAMEBUFFER,w),!(y.depthTexture&&y.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let X=i.get(y.depthTexture);X.__renderTarget=y,(!X.__webglTexture||y.depthTexture.image.width!==y.width||y.depthTexture.image.height!==y.height)&&(y.depthTexture.image.width=y.width,y.depthTexture.image.height=y.height,y.depthTexture.needsUpdate=!0),J(y.depthTexture,0);let K=X.__webglTexture,j=Ke(y);if(y.depthTexture.format===Ks)Qe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,K,0,j):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,K,0);else if(y.depthTexture.format===so)Qe(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,K,0,j):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,K,0);else throw new Error("Unknown depthTexture format")}function Le(w){let y=i.get(w),L=w.isWebGLCubeRenderTarget===!0;if(y.__boundDepthTexture!==w.depthTexture){let X=w.depthTexture;if(y.__depthDisposeCallback&&y.__depthDisposeCallback(),X){let K=()=>{delete y.__boundDepthTexture,delete y.__depthDisposeCallback,X.removeEventListener("dispose",K)};X.addEventListener("dispose",K),y.__depthDisposeCallback=K}y.__boundDepthTexture=X}if(w.depthTexture&&!y.__autoAllocateDepthBuffer){if(L)throw new Error("target.depthTexture not supported in Cube render targets");Re(y.__webglFramebuffer,w)}else if(L){y.__webglDepthbuffer=[];for(let X=0;X<6;X++)if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[X]),y.__webglDepthbuffer[X]===void 0)y.__webglDepthbuffer[X]=n.createRenderbuffer(),ce(y.__webglDepthbuffer[X],w,!1);else{let K=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,j=y.__webglDepthbuffer[X];n.bindRenderbuffer(n.RENDERBUFFER,j),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,j)}}else if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer),y.__webglDepthbuffer===void 0)y.__webglDepthbuffer=n.createRenderbuffer(),ce(y.__webglDepthbuffer,w,!1);else{let X=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,K=y.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,K),n.framebufferRenderbuffer(n.FRAMEBUFFER,X,n.RENDERBUFFER,K)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function $e(w,y,L){let X=i.get(w);y!==void 0&&be(X.__webglFramebuffer,w,w.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),L!==void 0&&Le(w)}function St(w){let y=w.texture,L=i.get(w),X=i.get(y);w.addEventListener("dispose",C);let K=w.textures,j=w.isWebGLCubeRenderTarget===!0,Me=K.length>1;if(Me||(X.__webglTexture===void 0&&(X.__webglTexture=n.createTexture()),X.__version=y.version,o.memory.textures++),j){L.__webglFramebuffer=[];for(let le=0;le<6;le++)if(y.mipmaps&&y.mipmaps.length>0){L.__webglFramebuffer[le]=[];for(let pe=0;pe<y.mipmaps.length;pe++)L.__webglFramebuffer[le][pe]=n.createFramebuffer()}else L.__webglFramebuffer[le]=n.createFramebuffer()}else{if(y.mipmaps&&y.mipmaps.length>0){L.__webglFramebuffer=[];for(let le=0;le<y.mipmaps.length;le++)L.__webglFramebuffer[le]=n.createFramebuffer()}else L.__webglFramebuffer=n.createFramebuffer();if(Me)for(let le=0,pe=K.length;le<pe;le++){let it=i.get(K[le]);it.__webglTexture===void 0&&(it.__webglTexture=n.createTexture(),o.memory.textures++)}if(w.samples>0&&Qe(w)===!1){L.__webglMultisampledFramebuffer=n.createFramebuffer(),L.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,L.__webglMultisampledFramebuffer);for(let le=0;le<K.length;le++){let pe=K[le];L.__webglColorRenderbuffer[le]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,L.__webglColorRenderbuffer[le]);let it=s.convert(pe.format,pe.colorSpace),te=s.convert(pe.type),me=E(pe.internalFormat,it,te,pe.colorSpace,w.isXRRenderTarget===!0),Ae=Ke(w);n.renderbufferStorageMultisample(n.RENDERBUFFER,Ae,me,w.width,w.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+le,n.RENDERBUFFER,L.__webglColorRenderbuffer[le])}n.bindRenderbuffer(n.RENDERBUFFER,null),w.depthBuffer&&(L.__webglDepthRenderbuffer=n.createRenderbuffer(),ce(L.__webglDepthRenderbuffer,w,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(j){t.bindTexture(n.TEXTURE_CUBE_MAP,X.__webglTexture),je(n.TEXTURE_CUBE_MAP,y);for(let le=0;le<6;le++)if(y.mipmaps&&y.mipmaps.length>0)for(let pe=0;pe<y.mipmaps.length;pe++)be(L.__webglFramebuffer[le][pe],w,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+le,pe);else be(L.__webglFramebuffer[le],w,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+le,0);m(y)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(Me){for(let le=0,pe=K.length;le<pe;le++){let it=K[le],te=i.get(it);t.bindTexture(n.TEXTURE_2D,te.__webglTexture),je(n.TEXTURE_2D,it),be(L.__webglFramebuffer,w,it,n.COLOR_ATTACHMENT0+le,n.TEXTURE_2D,0),m(it)&&p(n.TEXTURE_2D)}t.unbindTexture()}else{let le=n.TEXTURE_2D;if((w.isWebGL3DRenderTarget||w.isWebGLArrayRenderTarget)&&(le=w.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(le,X.__webglTexture),je(le,y),y.mipmaps&&y.mipmaps.length>0)for(let pe=0;pe<y.mipmaps.length;pe++)be(L.__webglFramebuffer[pe],w,y,n.COLOR_ATTACHMENT0,le,pe);else be(L.__webglFramebuffer,w,y,n.COLOR_ATTACHMENT0,le,0);m(y)&&p(le),t.unbindTexture()}w.depthBuffer&&Le(w)}function nt(w){let y=w.textures;for(let L=0,X=y.length;L<X;L++){let K=y[L];if(m(K)){let j=S(w),Me=i.get(K).__webglTexture;t.bindTexture(j,Me),p(j),t.unbindTexture()}}}let Nt=[],P=[];function Cn(w){if(w.samples>0){if(Qe(w)===!1){let y=w.textures,L=w.width,X=w.height,K=n.COLOR_BUFFER_BIT,j=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Me=i.get(w),le=y.length>1;if(le)for(let pe=0;pe<y.length;pe++)t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,Me.__webglMultisampledFramebuffer),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglFramebuffer);for(let pe=0;pe<y.length;pe++){if(w.resolveDepthBuffer&&(w.depthBuffer&&(K|=n.DEPTH_BUFFER_BIT),w.stencilBuffer&&w.resolveStencilBuffer&&(K|=n.STENCIL_BUFFER_BIT)),le){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,Me.__webglColorRenderbuffer[pe]);let it=i.get(y[pe]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,it,0)}n.blitFramebuffer(0,0,L,X,0,0,L,X,K,n.NEAREST),c===!0&&(Nt.length=0,P.length=0,Nt.push(n.COLOR_ATTACHMENT0+pe),w.depthBuffer&&w.resolveDepthBuffer===!1&&(Nt.push(j),P.push(j),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,P)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Nt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),le)for(let pe=0;pe<y.length;pe++){t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.RENDERBUFFER,Me.__webglColorRenderbuffer[pe]);let it=i.get(y[pe]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+pe,n.TEXTURE_2D,it,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglMultisampledFramebuffer)}else if(w.depthBuffer&&w.resolveDepthBuffer===!1&&c){let y=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[y])}}}function Ke(w){return Math.min(r.maxSamples,w.samples)}function Qe(w){let y=i.get(w);return w.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&y.__useRenderToTexture!==!1}function De(w){let y=o.render.frame;u.get(w)!==y&&(u.set(w,y),w.update())}function xt(w,y){let L=w.colorSpace,X=w.format,K=w.type;return w.isCompressedTexture===!0||w.isVideoTexture===!0||L!==fo&&L!==lr&&(rt.getTransfer(L)===ht?(X!==Yn||K!==Gi)&&console.warn("THREE.WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):console.error("THREE.WebGLTextures: Unsupported texture color space:",L)),y}function Ce(w){return typeof HTMLImageElement<"u"&&w instanceof HTMLImageElement?(l.width=w.naturalWidth||w.width,l.height=w.naturalHeight||w.height):typeof VideoFrame<"u"&&w instanceof VideoFrame?(l.width=w.displayWidth,l.height=w.displayHeight):(l.width=w.width,l.height=w.height),l}this.allocateTextureUnit=B,this.resetTextureUnits=W,this.setTexture2D=J,this.setTexture2DArray=q,this.setTexture3D=Q,this.setTextureCube=z,this.rebindTextures=$e,this.setupRenderTarget=St,this.updateRenderTargetMipmap=nt,this.updateMultisampleRenderTarget=Cn,this.setupDepthRenderbuffer=Le,this.setupFrameBufferTexture=be,this.useMultisampledRTT=Qe}function bN(n,e){function t(i,r=lr){let s,o=rt.getTransfer(r);if(i===Gi)return n.UNSIGNED_BYTE;if(i===pm)return n.UNSIGNED_SHORT_4_4_4_4;if(i===mm)return n.UNSIGNED_SHORT_5_5_5_1;if(i===ax)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===sx)return n.BYTE;if(i===ox)return n.SHORT;if(i===ya)return n.UNSIGNED_SHORT;if(i===hm)return n.INT;if(i===is)return n.UNSIGNED_INT;if(i===Bi)return n.FLOAT;if(i===Ma)return n.HALF_FLOAT;if(i===cx)return n.ALPHA;if(i===lx)return n.RGB;if(i===Yn)return n.RGBA;if(i===ux)return n.LUMINANCE;if(i===dx)return n.LUMINANCE_ALPHA;if(i===Ks)return n.DEPTH_COMPONENT;if(i===so)return n.DEPTH_STENCIL;if(i===fx)return n.RED;if(i===gm)return n.RED_INTEGER;if(i===hx)return n.RG;if(i===vm)return n.RG_INTEGER;if(i===ym)return n.RGBA_INTEGER;if(i===Fl||i===Ll||i===kl||i===Ul)if(o===ht)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Fl)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Ll)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===kl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Ul)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Fl)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Ll)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===kl)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Ul)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===up||i===dp||i===fp||i===hp)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===up)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===dp)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===fp)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===hp)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===pp||i===mp||i===gp)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===pp||i===mp)return o===ht?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===gp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC}else return null;if(i===vp||i===yp||i===_p||i===xp||i===bp||i===Mp||i===Ep||i===Sp||i===wp||i===Cp||i===Tp||i===Dp||i===Ap||i===Ip)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===vp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===yp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===_p)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===xp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===bp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Mp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Ep)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Sp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===wp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Cp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Tp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Dp)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Ap)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Ip)return o===ht?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Vl||i===Rp||i===Np)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Vl)return o===ht?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Rp)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Np)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===px||i===Pp||i===Op||i===Fp)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Vl)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Pp)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Op)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Fp)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===ro?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var Kp=class extends nn{constructor(e=[]){super(),this.isArrayCamera=!0,this.cameras=e}},Zs=class extends mr{constructor(){super(),this.isGroup=!0,this.type="Group"}},MN={type:"move"},va=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Zs,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Zs,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new O,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new O),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Zs,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new O,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new O),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let v of e.hand.values()){let m=t.getJointPose(v,i),p=this._getHandJoint(l,v);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],h=u.position.distanceTo(d.position),f=.02,g=.005;l.inputState.pinching&&h>f+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&h<=f-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(MN)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Zs;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},EN=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,SN=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,Qp=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t,i){if(this.texture===null){let r=new cs,s=e.properties.get(r);s.__webglTexture=t.texture,(t.depthNear!=i.depthNear||t.depthFar!=i.depthFar)&&(this.depthNear=t.depthNear,this.depthFar=t.depthFar),this.texture=r}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new li({vertexShader:EN,fragmentShader:SN,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Yt(new Jl(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},em=class extends hr{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,h=null,f=null,g=null,v=new Qp,m=t.getContextAttributes(),p=null,S=null,E=[],b=[],F=new st,T=null,C=new nn;C.viewport=new pt;let I=new nn;I.viewport=new pt;let M=[C,I],x=new Kp,D=null,W=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function($){let re=E[$];return re===void 0&&(re=new va,E[$]=re),re.getTargetRaySpace()},this.getControllerGrip=function($){let re=E[$];return re===void 0&&(re=new va,E[$]=re),re.getGripSpace()},this.getHand=function($){let re=E[$];return re===void 0&&(re=new va,E[$]=re),re.getHandSpace()};function B($){let re=b.indexOf($.inputSource);if(re===-1)return;let be=E[re];be!==void 0&&(be.update($.inputSource,$.frame,l||o),be.dispatchEvent({type:$.type,data:$.inputSource}))}function Z(){r.removeEventListener("select",B),r.removeEventListener("selectstart",B),r.removeEventListener("selectend",B),r.removeEventListener("squeeze",B),r.removeEventListener("squeezestart",B),r.removeEventListener("squeezeend",B),r.removeEventListener("end",Z),r.removeEventListener("inputsourceschange",J);for(let $=0;$<E.length;$++){let re=b[$];re!==null&&(b[$]=null,E[$].disconnect(re))}D=null,W=null,v.reset(),e.setRenderTarget(p),f=null,h=null,d=null,r=null,S=null,mt.stop(),i.isPresenting=!1,e.setPixelRatio(T),e.setSize(F.width,F.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function($){s=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function($){a=$,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function($){l=$},this.getBaseLayer=function(){return h!==null?h:f},this.getBinding=function(){return d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function($){return ds(this,null,function*(){if(r=$,r!==null){if(p=e.getRenderTarget(),r.addEventListener("select",B),r.addEventListener("selectstart",B),r.addEventListener("selectend",B),r.addEventListener("squeeze",B),r.addEventListener("squeezestart",B),r.addEventListener("squeezeend",B),r.addEventListener("end",Z),r.addEventListener("inputsourceschange",J),m.xrCompatible!==!0&&(yield t.makeXRCompatible()),T=e.getPixelRatio(),e.getSize(F),r.renderState.layers===void 0){let re={antialias:m.antialias,alpha:!0,depth:m.depth,stencil:m.stencil,framebufferScaleFactor:s};f=new XRWebGLLayer(r,t,re),r.updateRenderState({baseLayer:f}),e.setPixelRatio(1),e.setSize(f.framebufferWidth,f.framebufferHeight,!1),S=new Wi(f.framebufferWidth,f.framebufferHeight,{format:Yn,type:Gi,colorSpace:e.outputColorSpace,stencilBuffer:m.stencil})}else{let re=null,be=null,ce=null;m.depth&&(ce=m.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,re=m.stencil?so:Ks,be=m.stencil?ro:is);let Re={colorFormat:t.RGBA8,depthFormat:ce,scaleFactor:s};d=new XRWebGLBinding(r,t),h=d.createProjectionLayer(Re),r.updateRenderState({layers:[h]}),e.setPixelRatio(1),e.setSize(h.textureWidth,h.textureHeight,!1),S=new Wi(h.textureWidth,h.textureHeight,{format:Yn,type:Gi,depthTexture:new Ql(h.textureWidth,h.textureHeight,be,void 0,void 0,void 0,void 0,void 0,void 0,re),stencilBuffer:m.stencil,colorSpace:e.outputColorSpace,samples:m.antialias?4:0,resolveDepthBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=yield r.requestReferenceSpace(a),mt.setContext(r),mt.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return v.getDepthTexture()};function J($){for(let re=0;re<$.removed.length;re++){let be=$.removed[re],ce=b.indexOf(be);ce>=0&&(b[ce]=null,E[ce].disconnect(be))}for(let re=0;re<$.added.length;re++){let be=$.added[re],ce=b.indexOf(be);if(ce===-1){for(let Le=0;Le<E.length;Le++)if(Le>=b.length){b.push(be),ce=Le;break}else if(b[Le]===null){b[Le]=be,ce=Le;break}if(ce===-1)break}let Re=E[ce];Re&&Re.connect(be)}}let q=new O,Q=new O;function z($,re,be){q.setFromMatrixPosition(re.matrixWorld),Q.setFromMatrixPosition(be.matrixWorld);let ce=q.distanceTo(Q),Re=re.projectionMatrix.elements,Le=be.projectionMatrix.elements,$e=Re[14]/(Re[10]-1),St=Re[14]/(Re[10]+1),nt=(Re[9]+1)/Re[5],Nt=(Re[9]-1)/Re[5],P=(Re[8]-1)/Re[0],Cn=(Le[8]+1)/Le[0],Ke=$e*P,Qe=$e*Cn,De=ce/(-P+Cn),xt=De*-P;if(re.matrixWorld.decompose($.position,$.quaternion,$.scale),$.translateX(xt),$.translateZ(De),$.matrixWorld.compose($.position,$.quaternion,$.scale),$.matrixWorldInverse.copy($.matrixWorld).invert(),Re[10]===-1)$.projectionMatrix.copy(re.projectionMatrix),$.projectionMatrixInverse.copy(re.projectionMatrixInverse);else{let Ce=$e+De,w=St+De,y=Ke-xt,L=Qe+(ce-xt),X=nt*St/w*Ce,K=Nt*St/w*Ce;$.projectionMatrix.makePerspective(y,L,X,K,Ce,w),$.projectionMatrixInverse.copy($.projectionMatrix).invert()}}function ae($,re){re===null?$.matrixWorld.copy($.matrix):$.matrixWorld.multiplyMatrices(re.matrixWorld,$.matrix),$.matrixWorldInverse.copy($.matrixWorld).invert()}this.updateCamera=function($){if(r===null)return;let re=$.near,be=$.far;v.texture!==null&&(v.depthNear>0&&(re=v.depthNear),v.depthFar>0&&(be=v.depthFar)),x.near=I.near=C.near=re,x.far=I.far=C.far=be,(D!==x.near||W!==x.far)&&(r.updateRenderState({depthNear:x.near,depthFar:x.far}),D=x.near,W=x.far),C.layers.mask=$.layers.mask|2,I.layers.mask=$.layers.mask|4,x.layers.mask=C.layers.mask|I.layers.mask;let ce=$.parent,Re=x.cameras;ae(x,ce);for(let Le=0;Le<Re.length;Le++)ae(Re[Le],ce);Re.length===2?z(x,C,I):x.projectionMatrix.copy(C.projectionMatrix),he($,x,ce)};function he($,re,be){be===null?$.matrix.copy(re.matrixWorld):($.matrix.copy(be.matrixWorld),$.matrix.invert(),$.matrix.multiply(re.matrixWorld)),$.matrix.decompose($.position,$.quaternion,$.scale),$.updateMatrixWorld(!0),$.projectionMatrix.copy(re.projectionMatrix),$.projectionMatrixInverse.copy(re.projectionMatrixInverse),$.isPerspectiveCamera&&($.fov=kp*2*Math.atan(1/$.projectionMatrix.elements[5]),$.zoom=1)}this.getCamera=function(){return x},this.getFoveation=function(){if(!(h===null&&f===null))return c},this.setFoveation=function($){c=$,h!==null&&(h.fixedFoveation=$),f!==null&&f.fixedFoveation!==void 0&&(f.fixedFoveation=$)},this.hasDepthSensing=function(){return v.texture!==null},this.getDepthSensingMesh=function(){return v.getMesh(x)};let we=null;function je($,re){if(u=re.getViewerPose(l||o),g=re,u!==null){let be=u.views;f!==null&&(e.setRenderTargetFramebuffer(S,f.framebuffer),e.setRenderTarget(S));let ce=!1;be.length!==x.cameras.length&&(x.cameras.length=0,ce=!0);for(let Le=0;Le<be.length;Le++){let $e=be[Le],St=null;if(f!==null)St=f.getViewport($e);else{let Nt=d.getViewSubImage(h,$e);St=Nt.viewport,Le===0&&(e.setRenderTargetTextures(S,Nt.colorTexture,h.ignoreDepthValues?void 0:Nt.depthStencilTexture),e.setRenderTarget(S))}let nt=M[Le];nt===void 0&&(nt=new nn,nt.layers.enable(Le),nt.viewport=new pt,M[Le]=nt),nt.matrix.fromArray($e.transform.matrix),nt.matrix.decompose(nt.position,nt.quaternion,nt.scale),nt.projectionMatrix.fromArray($e.projectionMatrix),nt.projectionMatrixInverse.copy(nt.projectionMatrix).invert(),nt.viewport.set(St.x,St.y,St.width,St.height),Le===0&&(x.matrix.copy(nt.matrix),x.matrix.decompose(x.position,x.quaternion,x.scale)),ce===!0&&x.cameras.push(nt)}let Re=r.enabledFeatures;if(Re&&Re.includes("depth-sensing")){let Le=d.getDepthInformation(be[0]);Le&&Le.isValid&&Le.texture&&v.init(e,Le,r.renderState)}}for(let be=0;be<E.length;be++){let ce=b[be],Re=E[be];ce!==null&&Re!==void 0&&Re.update(ce,re,l||o)}we&&we($,re),re.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:re}),g=null}let mt=new _x;mt.setAnimationLoop(je),this.setAnimationLoop=function($){we=$},this.dispose=function(){}}},Zr=new ao,wN=new Tt;function CN(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,yx(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,S,E,b){p.isMeshBasicMaterial||p.isMeshLambertMaterial?s(m,p):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p)):p.isMeshStandardMaterial?(s(m,p),h(m,p),p.isMeshPhysicalMaterial&&f(m,p,b)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),v(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,S,E):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===_n&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===_n&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let S=e.get(p),E=S.envMap,b=S.envMapRotation;E&&(m.envMap.value=E,Zr.copy(b),Zr.x*=-1,Zr.y*=-1,Zr.z*=-1,E.isCubeTexture&&E.isRenderTargetTexture===!1&&(Zr.y*=-1,Zr.z*=-1),m.envMapRotation.value.setFromMatrix4(wN.makeRotationFromEuler(Zr)),m.flipEnvMap.value=E.isCubeTexture&&E.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,S,E){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*S,m.scale.value=E*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function h(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function f(m,p,S){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===_n&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=S.texture,m.transmissionSamplerSize.value.set(S.width,S.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function v(m,p){let S=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(S.matrixWorld),m.nearDistance.value=S.shadow.camera.near,m.farDistance.value=S.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function TN(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(S,E){let b=E.program;i.uniformBlockBinding(S,b)}function l(S,E){let b=r[S.id];b===void 0&&(g(S),b=u(S),r[S.id]=b,S.addEventListener("dispose",m));let F=E.program;i.updateUBOMapping(S,F);let T=e.render.frame;s[S.id]!==T&&(h(S),s[S.id]=T)}function u(S){let E=d();S.__bindingPointIndex=E;let b=n.createBuffer(),F=S.__size,T=S.usage;return n.bindBuffer(n.UNIFORM_BUFFER,b),n.bufferData(n.UNIFORM_BUFFER,F,T),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,E,b),b}function d(){for(let S=0;S<a;S++)if(o.indexOf(S)===-1)return o.push(S),S;return console.error("THREE.WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function h(S){let E=r[S.id],b=S.uniforms,F=S.__cache;n.bindBuffer(n.UNIFORM_BUFFER,E);for(let T=0,C=b.length;T<C;T++){let I=Array.isArray(b[T])?b[T]:[b[T]];for(let M=0,x=I.length;M<x;M++){let D=I[M];if(f(D,T,M,F)===!0){let W=D.__offset,B=Array.isArray(D.value)?D.value:[D.value],Z=0;for(let J=0;J<B.length;J++){let q=B[J],Q=v(q);typeof q=="number"||typeof q=="boolean"?(D.__data[0]=q,n.bufferSubData(n.UNIFORM_BUFFER,W+Z,D.__data)):q.isMatrix3?(D.__data[0]=q.elements[0],D.__data[1]=q.elements[1],D.__data[2]=q.elements[2],D.__data[3]=0,D.__data[4]=q.elements[3],D.__data[5]=q.elements[4],D.__data[6]=q.elements[5],D.__data[7]=0,D.__data[8]=q.elements[6],D.__data[9]=q.elements[7],D.__data[10]=q.elements[8],D.__data[11]=0):(q.toArray(D.__data,Z),Z+=Q.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,W,D.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function f(S,E,b,F){let T=S.value,C=E+"_"+b;if(F[C]===void 0)return typeof T=="number"||typeof T=="boolean"?F[C]=T:F[C]=T.clone(),!0;{let I=F[C];if(typeof T=="number"||typeof T=="boolean"){if(I!==T)return F[C]=T,!0}else if(I.equals(T)===!1)return I.copy(T),!0}return!1}function g(S){let E=S.uniforms,b=0,F=16;for(let C=0,I=E.length;C<I;C++){let M=Array.isArray(E[C])?E[C]:[E[C]];for(let x=0,D=M.length;x<D;x++){let W=M[x],B=Array.isArray(W.value)?W.value:[W.value];for(let Z=0,J=B.length;Z<J;Z++){let q=B[Z],Q=v(q),z=b%F,ae=z%Q.boundary,he=z+ae;b+=ae,he!==0&&F-he<Q.storage&&(b+=F-he),W.__data=new Float32Array(Q.storage/Float32Array.BYTES_PER_ELEMENT),W.__offset=b,b+=Q.storage}}}let T=b%F;return T>0&&(b+=F-T),S.__size=b,S.__cache={},this}function v(S){let E={boundary:0,storage:0};return typeof S=="number"||typeof S=="boolean"?(E.boundary=4,E.storage=4):S.isVector2?(E.boundary=8,E.storage=8):S.isVector3||S.isColor?(E.boundary=16,E.storage=12):S.isVector4?(E.boundary=16,E.storage=16):S.isMatrix3?(E.boundary=48,E.storage=48):S.isMatrix4?(E.boundary=64,E.storage=64):S.isTexture?console.warn("THREE.WebGLRenderer: Texture samplers can not be part of an uniforms group."):console.warn("THREE.WebGLRenderer: Unsupported uniform value type.",S),E}function m(S){let E=S.target;E.removeEventListener("dispose",m);let b=o.indexOf(E.__bindingPointIndex);o.splice(b,1),n.deleteBuffer(r[E.id]),delete r[E.id],delete s[E.id]}function p(){for(let S in r)n.deleteBuffer(r[S]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var eu=class{constructor(e={}){let{canvas:t=RD(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reverseDepthBuffer:h=!1}=e;this.isWebGLRenderer=!0;let f;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");f=i.getContextAttributes().alpha}else f=o;let g=new Uint32Array(4),v=new Int32Array(4),m=null,p=null,S=[],E=[];this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this._outputColorSpace=Un,this.toneMapping=dr,this.toneMappingExposure=1;let b=this,F=!1,T=0,C=0,I=null,M=-1,x=null,D=new pt,W=new pt,B=null,Z=new at(0),J=0,q=t.width,Q=t.height,z=1,ae=null,he=null,we=new pt(0,0,q,Q),je=new pt(0,0,q,Q),mt=!1,$=new xa,re=!1,be=!1,ce=new Tt,Re=new Tt,Le=new O,$e=new pt,St={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},nt=!1;function Nt(){return I===null?z:1}let P=i;function Cn(_,R){return t.getContext(_,R)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine","three.js r170"),t.addEventListener("webglcontextlost",Y,!1),t.addEventListener("webglcontextrestored",fe,!1),t.addEventListener("webglcontextcreationerror",ue,!1),P===null){let R="webgl2";if(P=Cn(R,_),P===null)throw Cn(R)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw console.error("THREE.WebGLRenderer: "+_.message),_}let Ke,Qe,De,xt,Ce,w,y,L,X,K,j,Me,le,pe,it,te,me,Ae,Pe,ge,et,Be,gt,A;function oe(){Ke=new G1(P),Ke.init(),Be=new bN(P,Ke),Qe=new k1(P,Ke,e,Be),De=new yN(P,Ke),Qe.reverseDepthBuffer&&h&&De.buffers.depth.setReversed(!0),xt=new $1(P),Ce=new sN,w=new xN(P,Ke,De,Ce,Qe,Be,xt),y=new V1(b),L=new z1(b),X=new QD(P),gt=new F1(P,X),K=new W1(P,X,xt,gt),j=new X1(P,K,X,xt),Pe=new q1(P,Qe,w),te=new U1(Ce),Me=new rN(b,y,L,Ke,Qe,gt,te),le=new CN(b,Ce),pe=new aN,it=new hN(Ke),Ae=new O1(b,y,L,De,j,f,c),me=new gN(b,j,Qe),A=new TN(P,xt,Qe,De),ge=new L1(P,Ke,xt),et=new j1(P,Ke,xt),xt.programs=Me.programs,b.capabilities=Qe,b.extensions=Ke,b.properties=Ce,b.renderLists=pe,b.shadowMap=me,b.state=De,b.info=xt}oe();let H=new em(b,P);this.xr=H,this.getContext=function(){return P},this.getContextAttributes=function(){return P.getContextAttributes()},this.forceContextLoss=function(){let _=Ke.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=Ke.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return z},this.setPixelRatio=function(_){_!==void 0&&(z=_,this.setSize(q,Q,!1))},this.getSize=function(_){return _.set(q,Q)},this.setSize=function(_,R,k=!0){if(H.isPresenting){console.warn("THREE.WebGLRenderer: Can't change size while VR device is presenting.");return}q=_,Q=R,t.width=Math.floor(_*z),t.height=Math.floor(R*z),k===!0&&(t.style.width=_+"px",t.style.height=R+"px"),this.setViewport(0,0,_,R)},this.getDrawingBufferSize=function(_){return _.set(q*z,Q*z).floor()},this.setDrawingBufferSize=function(_,R,k){q=_,Q=R,z=k,t.width=Math.floor(_*k),t.height=Math.floor(R*k),this.setViewport(0,0,_,R)},this.getCurrentViewport=function(_){return _.copy(D)},this.getViewport=function(_){return _.copy(we)},this.setViewport=function(_,R,k,U){_.isVector4?we.set(_.x,_.y,_.z,_.w):we.set(_,R,k,U),De.viewport(D.copy(we).multiplyScalar(z).round())},this.getScissor=function(_){return _.copy(je)},this.setScissor=function(_,R,k,U){_.isVector4?je.set(_.x,_.y,_.z,_.w):je.set(_,R,k,U),De.scissor(W.copy(je).multiplyScalar(z).round())},this.getScissorTest=function(){return mt},this.setScissorTest=function(_){De.setScissorTest(mt=_)},this.setOpaqueSort=function(_){ae=_},this.setTransparentSort=function(_){he=_},this.getClearColor=function(_){return _.copy(Ae.getClearColor())},this.setClearColor=function(){Ae.setClearColor.apply(Ae,arguments)},this.getClearAlpha=function(){return Ae.getClearAlpha()},this.setClearAlpha=function(){Ae.setClearAlpha.apply(Ae,arguments)},this.clear=function(_=!0,R=!0,k=!0){let U=0;if(_){let N=!1;if(I!==null){let ie=I.texture.format;N=ie===ym||ie===vm||ie===gm}if(N){let ie=I.texture.type,de=ie===Gi||ie===is||ie===ya||ie===ro||ie===pm||ie===mm,ye=Ae.getClearColor(),_e=Ae.getClearAlpha(),Oe=ye.r,Ue=ye.g,xe=ye.b;de?(g[0]=Oe,g[1]=Ue,g[2]=xe,g[3]=_e,P.clearBufferuiv(P.COLOR,0,g)):(v[0]=Oe,v[1]=Ue,v[2]=xe,v[3]=_e,P.clearBufferiv(P.COLOR,0,v))}else U|=P.COLOR_BUFFER_BIT}R&&(U|=P.DEPTH_BUFFER_BIT),k&&(U|=P.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),P.clear(U)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",Y,!1),t.removeEventListener("webglcontextrestored",fe,!1),t.removeEventListener("webglcontextcreationerror",ue,!1),pe.dispose(),it.dispose(),Ce.dispose(),y.dispose(),L.dispose(),j.dispose(),gt.dispose(),A.dispose(),Me.dispose(),H.dispose(),H.removeEventListener("sessionstart",Rm),H.removeEventListener("sessionend",Nm),_r.stop()};function Y(_){_.preventDefault(),console.log("THREE.WebGLRenderer: Context Lost."),F=!0}function fe(){console.log("THREE.WebGLRenderer: Context Restored."),F=!1;let _=xt.autoReset,R=me.enabled,k=me.autoUpdate,U=me.needsUpdate,N=me.type;oe(),xt.autoReset=_,me.enabled=R,me.autoUpdate=k,me.needsUpdate=U,me.type=N}function ue(_){console.error("THREE.WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function ke(_){let R=_.target;R.removeEventListener("dispose",ke),Dt(R)}function Dt(_){Zt(_),Ce.remove(_)}function Zt(_){let R=Ce.get(_).programs;R!==void 0&&(R.forEach(function(k){Me.releaseProgram(k)}),_.isShaderMaterial&&Me.releaseShaderCache(_))}this.renderBufferDirect=function(_,R,k,U,N,ie){R===null&&(R=St);let de=N.isMesh&&N.matrixWorld.determinant()<0,ye=jx(_,R,k,U,N);De.setMaterial(U,de);let _e=k.index,Oe=1;if(U.wireframe===!0){if(_e=K.getWireframeAttribute(k),_e===void 0)return;Oe=2}let Ue=k.drawRange,xe=k.attributes.position,ot=Ue.start*Oe,vt=(Ue.start+Ue.count)*Oe;ie!==null&&(ot=Math.max(ot,ie.start*Oe),vt=Math.min(vt,(ie.start+ie.count)*Oe)),_e!==null?(ot=Math.max(ot,0),vt=Math.min(vt,_e.count)):xe!=null&&(ot=Math.max(ot,0),vt=Math.min(vt,xe.count));let bt=vt-ot;if(bt<0||bt===1/0)return;gt.setup(N,U,ye,k,_e);let cn,ct=ge;if(_e!==null&&(cn=X.get(_e),ct=et,ct.setIndex(cn)),N.isMesh)U.wireframe===!0?(De.setLineWidth(U.wireframeLinewidth*Nt()),ct.setMode(P.LINES)):ct.setMode(P.TRIANGLES);else if(N.isLine){let Ee=U.linewidth;Ee===void 0&&(Ee=1),De.setLineWidth(Ee*Nt()),N.isLineSegments?ct.setMode(P.LINES):N.isLineLoop?ct.setMode(P.LINE_LOOP):ct.setMode(P.LINE_STRIP)}else N.isPoints?ct.setMode(P.POINTS):N.isSprite&&ct.setMode(P.TRIANGLES);if(N.isBatchedMesh)if(N._multiDrawInstances!==null)ct.renderMultiDrawInstances(N._multiDrawStarts,N._multiDrawCounts,N._multiDrawCount,N._multiDrawInstances);else if(Ke.get("WEBGL_multi_draw"))ct.renderMultiDraw(N._multiDrawStarts,N._multiDrawCounts,N._multiDrawCount);else{let Ee=N._multiDrawStarts,fi=N._multiDrawCounts,lt=N._multiDrawCount,Bn=_e?X.get(_e).bytesPerElement:1,us=Ce.get(U).currentProgram.getUniforms();for(let bn=0;bn<lt;bn++)us.setValue(P,"_gl_DrawID",bn),ct.render(Ee[bn]/Bn,fi[bn])}else if(N.isInstancedMesh)ct.renderInstances(ot,bt,N.count);else if(k.isInstancedBufferGeometry){let Ee=k._maxInstanceCount!==void 0?k._maxInstanceCount:1/0,fi=Math.min(k.instanceCount,Ee);ct.renderInstances(ot,bt,fi)}else ct.render(ot,bt)};function ut(_,R,k){_.transparent===!0&&_.side===Vi&&_.forceSinglePass===!1?(_.side=_n,_.needsUpdate=!0,Na(_,R,k),_.side=fr,_.needsUpdate=!0,Na(_,R,k),_.side=Vi):Na(_,R,k)}this.compile=function(_,R,k=null){k===null&&(k=_),p=it.get(k),p.init(R),E.push(p),k.traverseVisible(function(N){N.isLight&&N.layers.test(R.layers)&&(p.pushLight(N),N.castShadow&&p.pushShadow(N))}),_!==k&&_.traverseVisible(function(N){N.isLight&&N.layers.test(R.layers)&&(p.pushLight(N),N.castShadow&&p.pushShadow(N))}),p.setupLights();let U=new Set;return _.traverse(function(N){if(!(N.isMesh||N.isPoints||N.isLine||N.isSprite))return;let ie=N.material;if(ie)if(Array.isArray(ie))for(let de=0;de<ie.length;de++){let ye=ie[de];ut(ye,k,N),U.add(ye)}else ut(ie,k,N),U.add(ie)}),E.pop(),p=null,U},this.compileAsync=function(_,R,k=null){let U=this.compile(_,R,k);return new Promise(N=>{function ie(){if(U.forEach(function(de){Ce.get(de).currentProgram.isReady()&&U.delete(de)}),U.size===0){N(_);return}setTimeout(ie,10)}Ke.get("KHR_parallel_shader_compile")!==null?ie():setTimeout(ie,10)})};let Vn=null;function di(_){Vn&&Vn(_)}function Rm(){_r.stop()}function Nm(){_r.start()}let _r=new _x;_r.setAnimationLoop(di),typeof self<"u"&&_r.setContext(self),this.setAnimationLoop=function(_){Vn=_,H.setAnimationLoop(_),_===null?_r.stop():_r.start()},H.addEventListener("sessionstart",Rm),H.addEventListener("sessionend",Nm),this.render=function(_,R){if(R!==void 0&&R.isCamera!==!0){console.error("THREE.WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(F===!0)return;if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),R.parent===null&&R.matrixWorldAutoUpdate===!0&&R.updateMatrixWorld(),H.enabled===!0&&H.isPresenting===!0&&(H.cameraAutoUpdate===!0&&H.updateCamera(R),R=H.getCamera()),_.isScene===!0&&_.onBeforeRender(b,_,R,I),p=it.get(_,E.length),p.init(R),E.push(p),Re.multiplyMatrices(R.projectionMatrix,R.matrixWorldInverse),$.setFromProjectionMatrix(Re),be=this.localClippingEnabled,re=te.init(this.clippingPlanes,be),m=pe.get(_,S.length),m.init(),S.push(m),H.enabled===!0&&H.isPresenting===!0){let ie=b.xr.getDepthSensingMesh();ie!==null&&Ru(ie,R,-1/0,b.sortObjects)}Ru(_,R,0,b.sortObjects),m.finish(),b.sortObjects===!0&&m.sort(ae,he),nt=H.enabled===!1||H.isPresenting===!1||H.hasDepthSensing()===!1,nt&&Ae.addToRenderList(m,_),this.info.render.frame++,re===!0&&te.beginShadows();let k=p.state.shadowsArray;me.render(k,_,R),re===!0&&te.endShadows(),this.info.autoReset===!0&&this.info.reset();let U=m.opaque,N=m.transmissive;if(p.setupLights(),R.isArrayCamera){let ie=R.cameras;if(N.length>0)for(let de=0,ye=ie.length;de<ye;de++){let _e=ie[de];Om(U,N,_,_e)}nt&&Ae.render(_);for(let de=0,ye=ie.length;de<ye;de++){let _e=ie[de];Pm(m,_,_e,_e.viewport)}}else N.length>0&&Om(U,N,_,R),nt&&Ae.render(_),Pm(m,_,R);I!==null&&(w.updateMultisampleRenderTarget(I),w.updateRenderTargetMipmap(I)),_.isScene===!0&&_.onAfterRender(b,_,R),gt.resetDefaultState(),M=-1,x=null,E.pop(),E.length>0?(p=E[E.length-1],re===!0&&te.setGlobalState(b.clippingPlanes,p.state.camera)):p=null,S.pop(),S.length>0?m=S[S.length-1]:m=null};function Ru(_,R,k,U){if(_.visible===!1)return;if(_.layers.test(R.layers)){if(_.isGroup)k=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(R);else if(_.isLight)p.pushLight(_),_.castShadow&&p.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||$.intersectsSprite(_)){U&&$e.setFromMatrixPosition(_.matrixWorld).applyMatrix4(Re);let de=j.update(_),ye=_.material;ye.visible&&m.push(_,de,ye,k,$e.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||$.intersectsObject(_))){let de=j.update(_),ye=_.material;if(U&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),$e.copy(_.boundingSphere.center)):(de.boundingSphere===null&&de.computeBoundingSphere(),$e.copy(de.boundingSphere.center)),$e.applyMatrix4(_.matrixWorld).applyMatrix4(Re)),Array.isArray(ye)){let _e=de.groups;for(let Oe=0,Ue=_e.length;Oe<Ue;Oe++){let xe=_e[Oe],ot=ye[xe.materialIndex];ot&&ot.visible&&m.push(_,de,ot,k,$e.z,xe)}}else ye.visible&&m.push(_,de,ye,k,$e.z,null)}}let ie=_.children;for(let de=0,ye=ie.length;de<ye;de++)Ru(ie[de],R,k,U)}function Pm(_,R,k,U){let N=_.opaque,ie=_.transmissive,de=_.transparent;p.setupLightsView(k),re===!0&&te.setGlobalState(b.clippingPlanes,k),U&&De.viewport(D.copy(U)),N.length>0&&Ra(N,R,k),ie.length>0&&Ra(ie,R,k),de.length>0&&Ra(de,R,k),De.buffers.depth.setTest(!0),De.buffers.depth.setMask(!0),De.buffers.color.setMask(!0),De.setPolygonOffset(!1)}function Om(_,R,k,U){if((k.isScene===!0?k.overrideMaterial:null)!==null)return;p.state.transmissionRenderTarget[U.id]===void 0&&(p.state.transmissionRenderTarget[U.id]=new Wi(1,1,{generateMipmaps:!0,type:Ke.has("EXT_color_buffer_half_float")||Ke.has("EXT_color_buffer_float")?Ma:Gi,minFilter:ns,samples:4,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:rt.workingColorSpace}));let ie=p.state.transmissionRenderTarget[U.id],de=U.viewport||D;ie.setSize(de.z,de.w);let ye=b.getRenderTarget();b.setRenderTarget(ie),b.getClearColor(Z),J=b.getClearAlpha(),J<1&&b.setClearColor(16777215,.5),b.clear(),nt&&Ae.render(k);let _e=b.toneMapping;b.toneMapping=dr;let Oe=U.viewport;if(U.viewport!==void 0&&(U.viewport=void 0),p.setupLightsView(U),re===!0&&te.setGlobalState(b.clippingPlanes,U),Ra(_,k,U),w.updateMultisampleRenderTarget(ie),w.updateRenderTargetMipmap(ie),Ke.has("WEBGL_multisampled_render_to_texture")===!1){let Ue=!1;for(let xe=0,ot=R.length;xe<ot;xe++){let vt=R[xe],bt=vt.object,cn=vt.geometry,ct=vt.material,Ee=vt.group;if(ct.side===Vi&&bt.layers.test(U.layers)){let fi=ct.side;ct.side=_n,ct.needsUpdate=!0,Fm(bt,k,U,cn,ct,Ee),ct.side=fi,ct.needsUpdate=!0,Ue=!0}}Ue===!0&&(w.updateMultisampleRenderTarget(ie),w.updateRenderTargetMipmap(ie))}b.setRenderTarget(ye),b.setClearColor(Z,J),Oe!==void 0&&(U.viewport=Oe),b.toneMapping=_e}function Ra(_,R,k){let U=R.isScene===!0?R.overrideMaterial:null;for(let N=0,ie=_.length;N<ie;N++){let de=_[N],ye=de.object,_e=de.geometry,Oe=U===null?de.material:U,Ue=de.group;ye.layers.test(k.layers)&&Fm(ye,R,k,_e,Oe,Ue)}}function Fm(_,R,k,U,N,ie){_.onBeforeRender(b,R,k,U,N,ie),_.modelViewMatrix.multiplyMatrices(k.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),N.onBeforeRender(b,R,k,U,_,ie),N.transparent===!0&&N.side===Vi&&N.forceSinglePass===!1?(N.side=_n,N.needsUpdate=!0,b.renderBufferDirect(k,R,U,N,_,ie),N.side=fr,N.needsUpdate=!0,b.renderBufferDirect(k,R,U,N,_,ie),N.side=Vi):b.renderBufferDirect(k,R,U,N,_,ie),_.onAfterRender(b,R,k,U,N,ie)}function Na(_,R,k){R.isScene!==!0&&(R=St);let U=Ce.get(_),N=p.state.lights,ie=p.state.shadowsArray,de=N.state.version,ye=Me.getParameters(_,N.state,ie,R,k),_e=Me.getProgramCacheKey(ye),Oe=U.programs;U.environment=_.isMeshStandardMaterial?R.environment:null,U.fog=R.fog,U.envMap=(_.isMeshStandardMaterial?L:y).get(_.envMap||U.environment),U.envMapRotation=U.environment!==null&&_.envMap===null?R.environmentRotation:_.envMapRotation,Oe===void 0&&(_.addEventListener("dispose",ke),Oe=new Map,U.programs=Oe);let Ue=Oe.get(_e);if(Ue!==void 0){if(U.currentProgram===Ue&&U.lightsStateVersion===de)return km(_,ye),Ue}else ye.uniforms=Me.getUniforms(_),_.onBeforeCompile(ye,b),Ue=Me.acquireProgram(ye,_e),Oe.set(_e,Ue),U.uniforms=ye.uniforms;let xe=U.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(xe.clippingPlanes=te.uniform),km(_,ye),U.needsLights=qx(_),U.lightsStateVersion=de,U.needsLights&&(xe.ambientLightColor.value=N.state.ambient,xe.lightProbe.value=N.state.probe,xe.directionalLights.value=N.state.directional,xe.directionalLightShadows.value=N.state.directionalShadow,xe.spotLights.value=N.state.spot,xe.spotLightShadows.value=N.state.spotShadow,xe.rectAreaLights.value=N.state.rectArea,xe.ltc_1.value=N.state.rectAreaLTC1,xe.ltc_2.value=N.state.rectAreaLTC2,xe.pointLights.value=N.state.point,xe.pointLightShadows.value=N.state.pointShadow,xe.hemisphereLights.value=N.state.hemi,xe.directionalShadowMap.value=N.state.directionalShadowMap,xe.directionalShadowMatrix.value=N.state.directionalShadowMatrix,xe.spotShadowMap.value=N.state.spotShadowMap,xe.spotLightMatrix.value=N.state.spotLightMatrix,xe.spotLightMap.value=N.state.spotLightMap,xe.pointShadowMap.value=N.state.pointShadowMap,xe.pointShadowMatrix.value=N.state.pointShadowMatrix),U.currentProgram=Ue,U.uniformsList=null,Ue}function Lm(_){if(_.uniformsList===null){let R=_.currentProgram.getUniforms();_.uniformsList=eo.seqWithValue(R.seq,_.uniforms)}return _.uniformsList}function km(_,R){let k=Ce.get(_);k.outputColorSpace=R.outputColorSpace,k.batching=R.batching,k.batchingColor=R.batchingColor,k.instancing=R.instancing,k.instancingColor=R.instancingColor,k.instancingMorph=R.instancingMorph,k.skinning=R.skinning,k.morphTargets=R.morphTargets,k.morphNormals=R.morphNormals,k.morphColors=R.morphColors,k.morphTargetsCount=R.morphTargetsCount,k.numClippingPlanes=R.numClippingPlanes,k.numIntersection=R.numClipIntersection,k.vertexAlphas=R.vertexAlphas,k.vertexTangents=R.vertexTangents,k.toneMapping=R.toneMapping}function jx(_,R,k,U,N){R.isScene!==!0&&(R=St),w.resetTextureUnits();let ie=R.fog,de=U.isMeshStandardMaterial?R.environment:null,ye=I===null?b.outputColorSpace:I.isXRRenderTarget===!0?I.texture.colorSpace:fo,_e=(U.isMeshStandardMaterial?L:y).get(U.envMap||de),Oe=U.vertexColors===!0&&!!k.attributes.color&&k.attributes.color.itemSize===4,Ue=!!k.attributes.tangent&&(!!U.normalMap||U.anisotropy>0),xe=!!k.morphAttributes.position,ot=!!k.morphAttributes.normal,vt=!!k.morphAttributes.color,bt=dr;U.toneMapped&&(I===null||I.isXRRenderTarget===!0)&&(bt=b.toneMapping);let cn=k.morphAttributes.position||k.morphAttributes.normal||k.morphAttributes.color,ct=cn!==void 0?cn.length:0,Ee=Ce.get(U),fi=p.state.lights;if(re===!0&&(be===!0||_!==x)){let Tn=_===x&&U.id===M;te.setState(U,_,Tn)}let lt=!1;U.version===Ee.__version?(Ee.needsLights&&Ee.lightsStateVersion!==fi.state.version||Ee.outputColorSpace!==ye||N.isBatchedMesh&&Ee.batching===!1||!N.isBatchedMesh&&Ee.batching===!0||N.isBatchedMesh&&Ee.batchingColor===!0&&N.colorTexture===null||N.isBatchedMesh&&Ee.batchingColor===!1&&N.colorTexture!==null||N.isInstancedMesh&&Ee.instancing===!1||!N.isInstancedMesh&&Ee.instancing===!0||N.isSkinnedMesh&&Ee.skinning===!1||!N.isSkinnedMesh&&Ee.skinning===!0||N.isInstancedMesh&&Ee.instancingColor===!0&&N.instanceColor===null||N.isInstancedMesh&&Ee.instancingColor===!1&&N.instanceColor!==null||N.isInstancedMesh&&Ee.instancingMorph===!0&&N.morphTexture===null||N.isInstancedMesh&&Ee.instancingMorph===!1&&N.morphTexture!==null||Ee.envMap!==_e||U.fog===!0&&Ee.fog!==ie||Ee.numClippingPlanes!==void 0&&(Ee.numClippingPlanes!==te.numPlanes||Ee.numIntersection!==te.numIntersection)||Ee.vertexAlphas!==Oe||Ee.vertexTangents!==Ue||Ee.morphTargets!==xe||Ee.morphNormals!==ot||Ee.morphColors!==vt||Ee.toneMapping!==bt||Ee.morphTargetsCount!==ct)&&(lt=!0):(lt=!0,Ee.__version=U.version);let Bn=Ee.currentProgram;lt===!0&&(Bn=Na(U,R,N));let us=!1,bn=!1,_o=!1,Mt=Bn.getUniforms(),Kn=Ee.uniforms;if(De.useProgram(Bn.program)&&(us=!0,bn=!0,_o=!0),U.id!==M&&(M=U.id,bn=!0),us||x!==_){De.buffers.depth.getReversed()?(ce.copy(_.projectionMatrix),PD(ce),OD(ce),Mt.setValue(P,"projectionMatrix",ce)):Mt.setValue(P,"projectionMatrix",_.projectionMatrix),Mt.setValue(P,"viewMatrix",_.matrixWorldInverse);let $i=Mt.map.cameraPosition;$i!==void 0&&$i.setValue(P,Le.setFromMatrixPosition(_.matrixWorld)),Qe.logarithmicDepthBuffer&&Mt.setValue(P,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(U.isMeshPhongMaterial||U.isMeshToonMaterial||U.isMeshLambertMaterial||U.isMeshBasicMaterial||U.isMeshStandardMaterial||U.isShaderMaterial)&&Mt.setValue(P,"isOrthographic",_.isOrthographicCamera===!0),x!==_&&(x=_,bn=!0,_o=!0)}if(N.isSkinnedMesh){Mt.setOptional(P,N,"bindMatrix"),Mt.setOptional(P,N,"bindMatrixInverse");let Tn=N.skeleton;Tn&&(Tn.boneTexture===null&&Tn.computeBoneTexture(),Mt.setValue(P,"boneTexture",Tn.boneTexture,w))}N.isBatchedMesh&&(Mt.setOptional(P,N,"batchingTexture"),Mt.setValue(P,"batchingTexture",N._matricesTexture,w),Mt.setOptional(P,N,"batchingIdTexture"),Mt.setValue(P,"batchingIdTexture",N._indirectTexture,w),Mt.setOptional(P,N,"batchingColorTexture"),N._colorsTexture!==null&&Mt.setValue(P,"batchingColorTexture",N._colorsTexture,w));let xo=k.morphAttributes;if((xo.position!==void 0||xo.normal!==void 0||xo.color!==void 0)&&Pe.update(N,k,Bn),(bn||Ee.receiveShadow!==N.receiveShadow)&&(Ee.receiveShadow=N.receiveShadow,Mt.setValue(P,"receiveShadow",N.receiveShadow)),U.isMeshGouraudMaterial&&U.envMap!==null&&(Kn.envMap.value=_e,Kn.flipEnvMap.value=_e.isCubeTexture&&_e.isRenderTargetTexture===!1?-1:1),U.isMeshStandardMaterial&&U.envMap===null&&R.environment!==null&&(Kn.envMapIntensity.value=R.environmentIntensity),bn&&(Mt.setValue(P,"toneMappingExposure",b.toneMappingExposure),Ee.needsLights&&$x(Kn,_o),ie&&U.fog===!0&&le.refreshFogUniforms(Kn,ie),le.refreshMaterialUniforms(Kn,U,z,Q,p.state.transmissionRenderTarget[_.id]),eo.upload(P,Lm(Ee),Kn,w)),U.isShaderMaterial&&U.uniformsNeedUpdate===!0&&(eo.upload(P,Lm(Ee),Kn,w),U.uniformsNeedUpdate=!1),U.isSpriteMaterial&&Mt.setValue(P,"center",N.center),Mt.setValue(P,"modelViewMatrix",N.modelViewMatrix),Mt.setValue(P,"normalMatrix",N.normalMatrix),Mt.setValue(P,"modelMatrix",N.matrixWorld),U.isShaderMaterial||U.isRawShaderMaterial){let Tn=U.uniformsGroups;for(let $i=0,qi=Tn.length;$i<qi;$i++){let Um=Tn[$i];A.update(Um,Bn),A.bind(Um,Bn)}}return Bn}function $x(_,R){_.ambientLightColor.needsUpdate=R,_.lightProbe.needsUpdate=R,_.directionalLights.needsUpdate=R,_.directionalLightShadows.needsUpdate=R,_.pointLights.needsUpdate=R,_.pointLightShadows.needsUpdate=R,_.spotLights.needsUpdate=R,_.spotLightShadows.needsUpdate=R,_.rectAreaLights.needsUpdate=R,_.hemisphereLights.needsUpdate=R}function qx(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return T},this.getActiveMipmapLevel=function(){return C},this.getRenderTarget=function(){return I},this.setRenderTargetTextures=function(_,R,k){Ce.get(_.texture).__webglTexture=R,Ce.get(_.depthTexture).__webglTexture=k;let U=Ce.get(_);U.__hasExternalTextures=!0,U.__autoAllocateDepthBuffer=k===void 0,U.__autoAllocateDepthBuffer||Ke.has("WEBGL_multisampled_render_to_texture")===!0&&(console.warn("THREE.WebGLRenderer: Render-to-texture extension was disabled because an external texture was provided"),U.__useRenderToTexture=!1)},this.setRenderTargetFramebuffer=function(_,R){let k=Ce.get(_);k.__webglFramebuffer=R,k.__useDefaultFramebuffer=R===void 0},this.setRenderTarget=function(_,R=0,k=0){I=_,T=R,C=k;let U=!0,N=null,ie=!1,de=!1;if(_){let _e=Ce.get(_);if(_e.__useDefaultFramebuffer!==void 0)De.bindFramebuffer(P.FRAMEBUFFER,null),U=!1;else if(_e.__webglFramebuffer===void 0)w.setupRenderTarget(_);else if(_e.__hasExternalTextures)w.rebindTextures(_,Ce.get(_.texture).__webglTexture,Ce.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let xe=_.depthTexture;if(_e.__boundDepthTexture!==xe){if(xe!==null&&Ce.has(xe)&&(_.width!==xe.image.width||_.height!==xe.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");w.setupDepthRenderbuffer(_)}}let Oe=_.texture;(Oe.isData3DTexture||Oe.isDataArrayTexture||Oe.isCompressedArrayTexture)&&(de=!0);let Ue=Ce.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Ue[R])?N=Ue[R][k]:N=Ue[R],ie=!0):_.samples>0&&w.useMultisampledRTT(_)===!1?N=Ce.get(_).__webglMultisampledFramebuffer:Array.isArray(Ue)?N=Ue[k]:N=Ue,D.copy(_.viewport),W.copy(_.scissor),B=_.scissorTest}else D.copy(we).multiplyScalar(z).floor(),W.copy(je).multiplyScalar(z).floor(),B=mt;if(De.bindFramebuffer(P.FRAMEBUFFER,N)&&U&&De.drawBuffers(_,N),De.viewport(D),De.scissor(W),De.setScissorTest(B),ie){let _e=Ce.get(_.texture);P.framebufferTexture2D(P.FRAMEBUFFER,P.COLOR_ATTACHMENT0,P.TEXTURE_CUBE_MAP_POSITIVE_X+R,_e.__webglTexture,k)}else if(de){let _e=Ce.get(_.texture),Oe=R||0;P.framebufferTextureLayer(P.FRAMEBUFFER,P.COLOR_ATTACHMENT0,_e.__webglTexture,k||0,Oe)}M=-1},this.readRenderTargetPixels=function(_,R,k,U,N,ie,de){if(!(_&&_.isWebGLRenderTarget)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let ye=Ce.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&de!==void 0&&(ye=ye[de]),ye){De.bindFramebuffer(P.FRAMEBUFFER,ye);try{let _e=_.texture,Oe=_e.format,Ue=_e.type;if(!Qe.textureFormatReadable(Oe)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Qe.textureTypeReadable(Ue)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}R>=0&&R<=_.width-U&&k>=0&&k<=_.height-N&&P.readPixels(R,k,U,N,Be.convert(Oe),Be.convert(Ue),ie)}finally{let _e=I!==null?Ce.get(I).__webglFramebuffer:null;De.bindFramebuffer(P.FRAMEBUFFER,_e)}}},this.readRenderTargetPixelsAsync=function(_,R,k,U,N,ie,de){return ds(this,null,function*(){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let ye=Ce.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&de!==void 0&&(ye=ye[de]),ye){let _e=_.texture,Oe=_e.format,Ue=_e.type;if(!Qe.textureFormatReadable(Oe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Qe.textureTypeReadable(Ue))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");if(R>=0&&R<=_.width-U&&k>=0&&k<=_.height-N){De.bindFramebuffer(P.FRAMEBUFFER,ye);let xe=P.createBuffer();P.bindBuffer(P.PIXEL_PACK_BUFFER,xe),P.bufferData(P.PIXEL_PACK_BUFFER,ie.byteLength,P.STREAM_READ),P.readPixels(R,k,U,N,Be.convert(Oe),Be.convert(Ue),0);let ot=I!==null?Ce.get(I).__webglFramebuffer:null;De.bindFramebuffer(P.FRAMEBUFFER,ot);let vt=P.fenceSync(P.SYNC_GPU_COMMANDS_COMPLETE,0);return P.flush(),yield ND(P,vt,4),P.bindBuffer(P.PIXEL_PACK_BUFFER,xe),P.getBufferSubData(P.PIXEL_PACK_BUFFER,0,ie),P.deleteBuffer(xe),P.deleteSync(vt),ie}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")}})},this.copyFramebufferToTexture=function(_,R=null,k=0){_.isTexture!==!0&&(ma("WebGLRenderer: copyFramebufferToTexture function signature has changed."),R=arguments[0]||null,_=arguments[1]);let U=Math.pow(2,-k),N=Math.floor(_.image.width*U),ie=Math.floor(_.image.height*U),de=R!==null?R.x:0,ye=R!==null?R.y:0;w.setTexture2D(_,0),P.copyTexSubImage2D(P.TEXTURE_2D,k,0,0,de,ye,N,ie),De.unbindTexture()},this.copyTextureToTexture=function(_,R,k=null,U=null,N=0){_.isTexture!==!0&&(ma("WebGLRenderer: copyTextureToTexture function signature has changed."),U=arguments[0]||null,_=arguments[1],R=arguments[2],N=arguments[3]||0,k=null);let ie,de,ye,_e,Oe,Ue,xe,ot,vt,bt=_.isCompressedTexture?_.mipmaps[N]:_.image;k!==null?(ie=k.max.x-k.min.x,de=k.max.y-k.min.y,ye=k.isBox3?k.max.z-k.min.z:1,_e=k.min.x,Oe=k.min.y,Ue=k.isBox3?k.min.z:0):(ie=bt.width,de=bt.height,ye=bt.depth||1,_e=0,Oe=0,Ue=0),U!==null?(xe=U.x,ot=U.y,vt=U.z):(xe=0,ot=0,vt=0);let cn=Be.convert(R.format),ct=Be.convert(R.type),Ee;R.isData3DTexture?(w.setTexture3D(R,0),Ee=P.TEXTURE_3D):R.isDataArrayTexture||R.isCompressedArrayTexture?(w.setTexture2DArray(R,0),Ee=P.TEXTURE_2D_ARRAY):(w.setTexture2D(R,0),Ee=P.TEXTURE_2D),P.pixelStorei(P.UNPACK_FLIP_Y_WEBGL,R.flipY),P.pixelStorei(P.UNPACK_PREMULTIPLY_ALPHA_WEBGL,R.premultiplyAlpha),P.pixelStorei(P.UNPACK_ALIGNMENT,R.unpackAlignment);let fi=P.getParameter(P.UNPACK_ROW_LENGTH),lt=P.getParameter(P.UNPACK_IMAGE_HEIGHT),Bn=P.getParameter(P.UNPACK_SKIP_PIXELS),us=P.getParameter(P.UNPACK_SKIP_ROWS),bn=P.getParameter(P.UNPACK_SKIP_IMAGES);P.pixelStorei(P.UNPACK_ROW_LENGTH,bt.width),P.pixelStorei(P.UNPACK_IMAGE_HEIGHT,bt.height),P.pixelStorei(P.UNPACK_SKIP_PIXELS,_e),P.pixelStorei(P.UNPACK_SKIP_ROWS,Oe),P.pixelStorei(P.UNPACK_SKIP_IMAGES,Ue);let _o=_.isDataArrayTexture||_.isData3DTexture,Mt=R.isDataArrayTexture||R.isData3DTexture;if(_.isRenderTargetTexture||_.isDepthTexture){let Kn=Ce.get(_),xo=Ce.get(R),Tn=Ce.get(Kn.__renderTarget),$i=Ce.get(xo.__renderTarget);De.bindFramebuffer(P.READ_FRAMEBUFFER,Tn.__webglFramebuffer),De.bindFramebuffer(P.DRAW_FRAMEBUFFER,$i.__webglFramebuffer);for(let qi=0;qi<ye;qi++)_o&&P.framebufferTextureLayer(P.READ_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Ce.get(_).__webglTexture,N,Ue+qi),_.isDepthTexture?(Mt&&P.framebufferTextureLayer(P.DRAW_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Ce.get(R).__webglTexture,N,vt+qi),P.blitFramebuffer(_e,Oe,ie,de,xe,ot,ie,de,P.DEPTH_BUFFER_BIT,P.NEAREST)):Mt?P.copyTexSubImage3D(Ee,N,xe,ot,vt+qi,_e,Oe,ie,de):P.copyTexSubImage2D(Ee,N,xe,ot,vt+qi,_e,Oe,ie,de);De.bindFramebuffer(P.READ_FRAMEBUFFER,null),De.bindFramebuffer(P.DRAW_FRAMEBUFFER,null)}else Mt?_.isDataTexture||_.isData3DTexture?P.texSubImage3D(Ee,N,xe,ot,vt,ie,de,ye,cn,ct,bt.data):R.isCompressedArrayTexture?P.compressedTexSubImage3D(Ee,N,xe,ot,vt,ie,de,ye,cn,bt.data):P.texSubImage3D(Ee,N,xe,ot,vt,ie,de,ye,cn,ct,bt):_.isDataTexture?P.texSubImage2D(P.TEXTURE_2D,N,xe,ot,ie,de,cn,ct,bt.data):_.isCompressedTexture?P.compressedTexSubImage2D(P.TEXTURE_2D,N,xe,ot,bt.width,bt.height,cn,bt.data):P.texSubImage2D(P.TEXTURE_2D,N,xe,ot,ie,de,cn,ct,bt);P.pixelStorei(P.UNPACK_ROW_LENGTH,fi),P.pixelStorei(P.UNPACK_IMAGE_HEIGHT,lt),P.pixelStorei(P.UNPACK_SKIP_PIXELS,Bn),P.pixelStorei(P.UNPACK_SKIP_ROWS,us),P.pixelStorei(P.UNPACK_SKIP_IMAGES,bn),N===0&&R.generateMipmaps&&P.generateMipmap(Ee),De.unbindTexture()},this.copyTextureToTexture3D=function(_,R,k=null,U=null,N=0){return _.isTexture!==!0&&(ma("WebGLRenderer: copyTextureToTexture3D function signature has changed."),k=arguments[0]||null,U=arguments[1]||null,_=arguments[2],R=arguments[3],N=arguments[4]||0),ma('WebGLRenderer: copyTextureToTexture3D function has been deprecated. Use "copyTextureToTexture" instead.'),this.copyTextureToTexture(_,R,k,U,N)},this.initRenderTarget=function(_){Ce.get(_).__webglFramebuffer===void 0&&w.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?w.setTextureCube(_,0):_.isData3DTexture?w.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?w.setTexture2DArray(_,0):w.setTexture2D(_,0),De.unbindTexture()},this.resetState=function(){T=0,C=0,I=null,De.reset(),gt.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Hi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorspace=rt._getDrawingBufferColorSpace(e),t.unpackColorSpace=rt._getUnpackColorSpace()}};var tu=class extends mr{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new ao,this.environmentIntensity=1,this.environmentRotation=new ao,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}};var ba=class extends ss{static get type(){return"PointsMaterial"}constructor(e){super(),this.isPointsMaterial=!0,this.color=new at(16777215),this.map=null,this.alphaMap=null,this.size=1,this.sizeAttenuation=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.size=e.size,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},J0=new Tt,tm=new jl,Nl=new oo,Pl=new O,nu=class extends mr{constructor(e=new wn,t=new ba){super(),this.isPoints=!0,this.type="Points",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Points.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),Nl.copy(i.boundingSphere),Nl.applyMatrix4(r),Nl.radius+=s,e.ray.intersectsSphere(Nl)===!1)return;J0.copy(r).invert(),tm.copy(e.ray).applyMatrix4(J0);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=i.index,d=i.attributes.position;if(l!==null){let h=Math.max(0,o.start),f=Math.min(l.count,o.start+o.count);for(let g=h,v=f;g<v;g++){let m=l.getX(g);Pl.fromBufferAttribute(d,m),K0(Pl,m,c,r,e,t,this)}}else{let h=Math.max(0,o.start),f=Math.min(d.count,o.start+o.count);for(let g=h,v=f;g<v;g++)Pl.fromBufferAttribute(d,g),K0(Pl,g,c,r,e,t,this)}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function K0(n,e,t,i,r,s,o){let a=tm.distanceSqToPoint(n);if(a<t){let c=new O;tm.closestPointToPoint(n,c),c.applyMatrix4(i);let l=r.ray.origin.distanceTo(c);if(l<r.near||l>r.far)return;s.push({distance:l,distanceToRay:Math.sqrt(a),point:c,index:e,face:null,faceIndex:null,barycoord:null,object:o})}}var nm=class n extends wn{constructor(e=[],t=[],i=1,r=0){super(),this.type="PolyhedronGeometry",this.parameters={vertices:e,indices:t,radius:i,detail:r};let s=[],o=[];a(r),l(i),u(),this.setAttribute("position",new $t(s,3)),this.setAttribute("normal",new $t(s.slice(),3)),this.setAttribute("uv",new $t(o,2)),r===0?this.computeVertexNormals():this.normalizeNormals();function a(S){let E=new O,b=new O,F=new O;for(let T=0;T<t.length;T+=3)f(t[T+0],E),f(t[T+1],b),f(t[T+2],F),c(E,b,F,S)}function c(S,E,b,F){let T=F+1,C=[];for(let I=0;I<=T;I++){C[I]=[];let M=S.clone().lerp(b,I/T),x=E.clone().lerp(b,I/T),D=T-I;for(let W=0;W<=D;W++)W===0&&I===T?C[I][W]=M:C[I][W]=M.clone().lerp(x,W/D)}for(let I=0;I<T;I++)for(let M=0;M<2*(T-I)-1;M++){let x=Math.floor(M/2);M%2===0?(h(C[I][x+1]),h(C[I+1][x]),h(C[I][x])):(h(C[I][x+1]),h(C[I+1][x+1]),h(C[I+1][x]))}}function l(S){let E=new O;for(let b=0;b<s.length;b+=3)E.x=s[b+0],E.y=s[b+1],E.z=s[b+2],E.normalize().multiplyScalar(S),s[b+0]=E.x,s[b+1]=E.y,s[b+2]=E.z}function u(){let S=new O;for(let E=0;E<s.length;E+=3){S.x=s[E+0],S.y=s[E+1],S.z=s[E+2];let b=m(S)/2/Math.PI+.5,F=p(S)/Math.PI+.5;o.push(b,1-F)}g(),d()}function d(){for(let S=0;S<o.length;S+=6){let E=o[S+0],b=o[S+2],F=o[S+4],T=Math.max(E,b,F),C=Math.min(E,b,F);T>.9&&C<.1&&(E<.2&&(o[S+0]+=1),b<.2&&(o[S+2]+=1),F<.2&&(o[S+4]+=1))}}function h(S){s.push(S.x,S.y,S.z)}function f(S,E){let b=S*3;E.x=e[b+0],E.y=e[b+1],E.z=e[b+2]}function g(){let S=new O,E=new O,b=new O,F=new O,T=new st,C=new st,I=new st;for(let M=0,x=0;M<s.length;M+=9,x+=6){S.set(s[M+0],s[M+1],s[M+2]),E.set(s[M+3],s[M+4],s[M+5]),b.set(s[M+6],s[M+7],s[M+8]),T.set(o[x+0],o[x+1]),C.set(o[x+2],o[x+3]),I.set(o[x+4],o[x+5]),F.copy(S).add(E).add(b).divideScalar(3);let D=m(F);v(T,x+0,S,D),v(C,x+2,E,D),v(I,x+4,b,D)}}function v(S,E,b,F){F<0&&S.x===1&&(o[E]=S.x-1),b.x===0&&b.z===0&&(o[E]=F/2/Math.PI+.5)}function m(S){return Math.atan2(S.z,-S.x)}function p(S){return Math.atan2(-S.y,Math.sqrt(S.x*S.x+S.z*S.z))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.vertices,e.indices,e.radius,e.details)}};var iu=class n extends nm{constructor(e=1,t=0){let i=(1+Math.sqrt(5))/2,r=[-1,i,0,1,i,0,-1,-i,0,1,-i,0,0,-1,i,0,1,i,0,-1,-i,0,1,-i,i,0,-1,i,0,1,-i,0,-1,-i,0,1],s=[0,11,5,0,5,1,0,1,7,0,7,10,0,10,11,1,5,9,5,11,4,11,10,2,10,7,6,7,1,8,3,9,4,3,4,2,3,2,6,3,6,8,3,8,9,4,9,5,2,4,11,6,2,10,8,6,7,9,8,1];super(r,s,e,t),this.type="IcosahedronGeometry",this.parameters={radius:e,detail:t}}static fromJSON(e){return new n(e.radius,e.detail)}};var ru=class n extends wn{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new O,h=new O,f=[],g=[],v=[],m=[];for(let p=0;p<=i;p++){let S=[],E=p/i,b=0;p===0&&o===0?b=.5/t:p===i&&c===Math.PI&&(b=-.5/t);for(let F=0;F<=t;F++){let T=F/t;d.x=-e*Math.cos(r+T*s)*Math.sin(o+E*a),d.y=e*Math.cos(o+E*a),d.z=e*Math.sin(r+T*s)*Math.sin(o+E*a),g.push(d.x,d.y,d.z),h.copy(d).normalize(),v.push(h.x,h.y,h.z),m.push(T+b,1-E),S.push(l++)}u.push(S)}for(let p=0;p<i;p++)for(let S=0;S<t;S++){let E=u[p][S+1],b=u[p][S],F=u[p+1][S],T=u[p+1][S+1];(p!==0||o>0)&&f.push(E,b,T),(p!==i-1||c<Math.PI)&&f.push(b,F,T)}this.setIndex(f),this.setAttribute("position",new $t(g,3)),this.setAttribute("normal",new $t(v,3)),this.setAttribute("uv",new $t(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};var lo=class n extends wn{constructor(e=1,t=.4,i=12,r=48,s=Math.PI*2){super(),this.type="TorusGeometry",this.parameters={radius:e,tube:t,radialSegments:i,tubularSegments:r,arc:s},i=Math.floor(i),r=Math.floor(r);let o=[],a=[],c=[],l=[],u=new O,d=new O,h=new O;for(let f=0;f<=i;f++)for(let g=0;g<=r;g++){let v=g/r*s,m=f/i*Math.PI*2;d.x=(e+t*Math.cos(m))*Math.cos(v),d.y=(e+t*Math.cos(m))*Math.sin(v),d.z=t*Math.sin(m),a.push(d.x,d.y,d.z),u.x=e*Math.cos(v),u.y=e*Math.sin(v),h.subVectors(d,u).normalize(),c.push(h.x,h.y,h.z),l.push(g/r),l.push(f/i)}for(let f=1;f<=i;f++)for(let g=1;g<=r;g++){let v=(r+1)*f+g-1,m=(r+1)*(f-1)+g-1,p=(r+1)*(f-1)+g,S=(r+1)*f+g;o.push(v,m,S),o.push(m,p,S)}this.setIndex(o),this.setAttribute("position",new $t(a,3)),this.setAttribute("normal",new $t(c,3)),this.setAttribute("uv",new $t(l,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.tube,e.radialSegments,e.tubularSegments,e.arc)}};function Ol(n,e,t){return!n||!t&&n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function DN(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}var uo=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},im=class extends uo{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:t0,endingEnd:t0}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case n0:s=e,a=2*t-i;break;case i0:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case n0:o=e,c=2*i-t;break;case i0:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,h=this._weightPrev,f=this._weightNext,g=(i-t)/(r-t),v=g*g,m=v*g,p=-h*m+2*h*v-h*g,S=(1+h)*m+(-1.5-2*h)*v+(-.5+h)*g+1,E=(-1-f)*m+(1.5+f)*v+.5*g,b=f*m-f*v;for(let F=0;F!==a;++F)s[F]=p*o[u+F]+S*o[l+F]+E*o[c+F]+b*o[d+F];return s}},rm=class extends uo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let h=0;h!==a;++h)s[h]=o[l+h]*d+o[c+h]*u;return s}},sm=class extends uo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Jn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Ol(t,this.TimeBufferType),this.values=Ol(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Ol(e.times,Array),values:Ol(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new sm(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new rm(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new im(this.times,this.values,this.getValueSize(),e)}setInterpolation(e){let t;switch(e){case Bl:t=this.InterpolantFactoryMethodDiscrete;break;case Lp:t=this.InterpolantFactoryMethodLinear;break;case xh:t=this.InterpolantFactoryMethodSmooth;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return console.warn("THREE.KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Bl;case this.InterpolantFactoryMethodLinear:return Lp;case this.InterpolantFactoryMethodSmooth:return xh}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(console.error("THREE.KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(console.error("THREE.KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){console.error("THREE.KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){console.error("THREE.KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&DN(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){console.error("THREE.KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===xh,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,h=d-i,f=d+i;for(let g=0;g!==i;++g){let v=t[d+g];if(v!==t[h+g]||v!==t[f+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,h=o*i;for(let f=0;f!==i;++f)t[h+f]=t[d+f]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Jn.prototype.TimeBufferType=Float32Array;Jn.prototype.ValueBufferType=Float32Array;Jn.prototype.DefaultInterpolation=Lp;var os=class extends Jn{constructor(e,t,i){super(e,t,i)}};os.prototype.ValueTypeName="bool";os.prototype.ValueBufferType=Array;os.prototype.DefaultInterpolation=Bl;os.prototype.InterpolantFactoryMethodLinear=void 0;os.prototype.InterpolantFactoryMethodSmooth=void 0;var om=class extends Jn{};om.prototype.ValueTypeName="color";var am=class extends Jn{};am.prototype.ValueTypeName="number";var cm=class extends uo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)pr.slerpFlat(s,0,o,l-a,o,l,c);return s}},su=class extends Jn{InterpolantFactoryMethodLinear(e){return new cm(this.times,this.values,this.getValueSize(),e)}};su.prototype.ValueTypeName="quaternion";su.prototype.InterpolantFactoryMethodSmooth=void 0;var as=class extends Jn{constructor(e,t,i){super(e,t,i)}};as.prototype.ValueTypeName="string";as.prototype.ValueBufferType=Array;as.prototype.DefaultInterpolation=Bl;as.prototype.InterpolantFactoryMethodLinear=void 0;as.prototype.InterpolantFactoryMethodSmooth=void 0;var lm=class extends Jn{};lm.prototype.ValueTypeName="vector";var ou=class extends mr{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new at(e),this.intensity=t}dispose(){}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,this.groundColor!==void 0&&(t.object.groundColor=this.groundColor.getHex()),this.distance!==void 0&&(t.object.distance=this.distance),this.angle!==void 0&&(t.object.angle=this.angle),this.decay!==void 0&&(t.object.decay=this.decay),this.penumbra!==void 0&&(t.object.penumbra=this.penumbra),this.shadow!==void 0&&(t.object.shadow=this.shadow.toJSON()),this.target!==void 0&&(t.object.target=this.target.uuid),t}};var Yh=new Tt,Q0=new O,ex=new O,um=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new st(512,512),this.map=null,this.mapPass=null,this.matrix=new Tt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new xa,this._frameExtents=new st(1,1),this._viewportCount=1,this._viewports=[new pt(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;Q0.setFromMatrixPosition(e.matrixWorld),t.position.copy(Q0),ex.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(ex),t.updateMatrixWorld(),Yh.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Yh),i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Yh)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.mapSize.copy(e.mapSize),this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}};var tx=new Tt,pa=new O,Zh=new O,dm=class extends um{constructor(){super(new nn(90,1,.5,500)),this.isPointLightShadow=!0,this._frameExtents=new st(4,2),this._viewportCount=6,this._viewports=[new pt(2,1,1,1),new pt(0,1,1,1),new pt(3,1,1,1),new pt(1,1,1,1),new pt(3,0,1,1),new pt(1,0,1,1)],this._cubeDirections=[new O(1,0,0),new O(-1,0,0),new O(0,0,1),new O(0,0,-1),new O(0,1,0),new O(0,-1,0)],this._cubeUps=[new O(0,1,0),new O(0,1,0),new O(0,1,0),new O(0,1,0),new O(0,0,1),new O(0,0,-1)]}updateMatrices(e,t=0){let i=this.camera,r=this.matrix,s=e.distance||i.far;s!==i.far&&(i.far=s,i.updateProjectionMatrix()),pa.setFromMatrixPosition(e.matrixWorld),i.position.copy(pa),Zh.copy(i.position),Zh.add(this._cubeDirections[t]),i.up.copy(this._cubeUps[t]),i.lookAt(Zh),i.updateMatrixWorld(),r.makeTranslation(-pa.x,-pa.y,-pa.z),tx.multiplyMatrices(i.projectionMatrix,i.matrixWorldInverse),this._frustum.setFromProjectionMatrix(tx)}},au=class extends ou{constructor(e,t,i=0,r=2){super(e,t),this.isPointLight=!0,this.type="PointLight",this.distance=i,this.decay=r,this.shadow=new dm}get power(){return this.intensity*4*Math.PI}set power(e){this.intensity=e/(4*Math.PI)}dispose(){this.shadow.dispose()}copy(e,t){return super.copy(e,t),this.distance=e.distance,this.decay=e.decay,this.shadow=e.shadow.clone(),this}};var cu=class extends ou{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var lu=class{constructor(e=!0){this.autoStart=e,this.startTime=0,this.oldTime=0,this.elapsedTime=0,this.running=!1}start(){this.startTime=nx(),this.oldTime=this.startTime,this.elapsedTime=0,this.running=!0}stop(){this.getElapsedTime(),this.running=!1,this.autoStart=!1}getElapsedTime(){return this.getDelta(),this.elapsedTime}getDelta(){let e=0;if(this.autoStart&&!this.running)return this.start(),0;if(this.running){let t=nx();e=(t-this.oldTime)/1e3,this.oldTime=t,this.elapsedTime+=e}return e}};function nx(){return performance.now()}var xm="\\[\\]\\.:\\/",AN=new RegExp("["+xm+"]","g"),bm="[^"+xm+"]",IN="[^"+xm.replace("\\.","")+"]",RN=/((?:WC+[\/:])*)/.source.replace("WC",bm),NN=/(WCOD+)?/.source.replace("WCOD",IN),PN=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",bm),ON=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",bm),FN=new RegExp("^"+RN+NN+PN+ON+"$"),LN=["material","materials","bones","map"],fm=class{constructor(e,t,i){let r=i||Ft.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Ft=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(AN,"")}static parseTrackName(t){let i=FN.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);LN.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){console.warn("THREE.PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){console.error("THREE.PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){console.error("THREE.PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){console.error("THREE.PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){console.error("THREE.PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){console.error("THREE.PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;console.error("THREE.PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.needsUpdate!==void 0?c=this.Versioning.NeedsUpdate:t.matrixWorldNeedsUpdate!==void 0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=fm,n})();Ft.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Ft.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Ft.prototype.GetterByBindingType=[Ft.prototype._getValue_direct,Ft.prototype._getValue_array,Ft.prototype._getValue_arrayElement,Ft.prototype._getValue_toArray];Ft.prototype.SetterByBindingTypeAndVersioning=[[Ft.prototype._setValue_direct,Ft.prototype._setValue_direct_setNeedsUpdate,Ft.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Ft.prototype._setValue_array,Ft.prototype._setValue_array_setNeedsUpdate,Ft.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Ft.prototype._setValue_arrayElement,Ft.prototype._setValue_arrayElement_setNeedsUpdate,Ft.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Ft.prototype._setValue_fromArray,Ft.prototype._setValue_fromArray_setNeedsUpdate,Ft.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var FV=new Float32Array(1);typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"170"}}));typeof window<"u"&&(window.__THREE__?console.warn("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="170");var UN=["hologramCanvas"],hu=class n{constructor(e){this.audioService=e;this.clock=new lu;this.mouse={x:0,y:0,targetX:0,targetY:0};this.state="idle";this.audioIntensity=0;this.animFrameId=0;this.subs=[];this.animate=()=>{this.animFrameId=requestAnimationFrame(this.animate);let e=this.clock.getDelta(),t=this.clock.getElapsedTime();this.mouse.x+=(this.mouse.targetX-this.mouse.x)*.05,this.mouse.y+=(this.mouse.targetY-this.mouse.y)*.05;let i=1;if(this.state==="thinking"&&(i=3.2),this.state==="executing"&&(i=2),this.state==="speaking"&&(i=1.5),this.coreMesh){this.coreMesh.rotation.x+=.4*e*i,this.coreMesh.rotation.y+=.6*e*i;let r=1+Math.sin(t*3)*.06+this.audioIntensity*.35;this.coreMesh.scale.set(r,r,r)}this.innerRing&&(this.innerRing.rotation.x=t*.8*i,this.innerRing.rotation.y=t*.5*i),this.middleRing&&(this.middleRing.rotation.y=-t*.7*i,this.middleRing.rotation.z=t*.4*i),this.outerRing&&(this.outerRing.rotation.x=-t*.3*i,this.outerRing.rotation.z=-t*.6*i),this.particleSystem&&(this.particleSystem.rotation.y=t*.15,this.particleSystem.rotation.x=t*.08),this.camera.position.x=this.mouse.x*2.5,this.camera.position.y=this.mouse.y*2,this.camera.lookAt(this.scene.position),this.renderer.render(this.scene,this.camera)}}ngAfterViewInit(){this.initThree(),this.subs.push(this.audioService.avatarState$.subscribe(e=>this.setHologramState(e)),this.audioService.audioIntensity$.subscribe(e=>this.audioIntensity=e))}ngOnDestroy(){cancelAnimationFrame(this.animFrameId),this.subs.forEach(e=>e.unsubscribe()),this.renderer&&this.renderer.dispose()}onWindowResize(){if(!this.canvasRef||!this.renderer||!this.camera)return;let e=this.canvasRef.nativeElement,t=e.clientWidth||300,i=e.clientHeight||240;this.camera.aspect=t/i,this.camera.updateProjectionMatrix(),this.renderer.setSize(t,i)}onMouseMove(e){this.mouse.targetX=e.clientX/window.innerWidth*2-1,this.mouse.targetY=-(e.clientY/window.innerHeight)*2+1}initThree(){let e=this.canvasRef.nativeElement,t=e.clientWidth||400,i=e.clientHeight||250;this.scene=new tu,this.camera=new nn(45,t/i,.1,1e3),this.camera.position.z=15,this.renderer=new eu({canvas:e,alpha:!0,antialias:!0}),this.renderer.setSize(t,i),this.renderer.setPixelRatio(Math.min(window.devicePixelRatio,2));let r=new cu(16777215,.8);this.scene.add(r);let s=new au(61695,2,50);s.position.set(0,0,10),this.scene.add(s),this.buildCore(),this.buildGimbalRings(),this.buildParticles(),this.animate()}buildCore(){let e=new iu(2.2,2),t=new ci({color:61695,wireframe:!0,transparent:!0,opacity:.85});this.coreMesh=new Yt(e,t),this.scene.add(this.coreMesh);let i=new ru(1.2,16,16),r=new ci({color:35071,transparent:!0,opacity:.6}),s=new Yt(i,r);this.coreMesh.add(s)}buildGimbalRings(){let e=new lo(3.2,.04,16,100),t=new ci({color:61695,transparent:!0,opacity:.7});this.innerRing=new Yt(e,t),this.scene.add(this.innerRing);let i=new lo(4.2,.05,16,100),r=new ci({color:35071,transparent:!0,opacity:.6});this.middleRing=new Yt(i,r),this.scene.add(this.middleRing);let s=new lo(5.2,.06,16,60),o=new ci({color:65437,wireframe:!0,transparent:!0,opacity:.5});this.outerRing=new Yt(s,o),this.scene.add(this.outerRing)}buildParticles(){let t=new wn,i=new Float32Array(600);for(let s=0;s<600;s+=3){let o=6+Math.random()*4,a=Math.random()*Math.PI*2,c=Math.acos(Math.random()*2-1);i[s]=o*Math.sin(c)*Math.cos(a),i[s+1]=o*Math.sin(c)*Math.sin(a),i[s+2]=o*Math.cos(c)}t.setAttribute("position",new xn(i,3));let r=new ba({color:61695,size:.12,transparent:!0,opacity:.75});this.particleSystem=new nu(t,r),this.scene.add(this.particleSystem)}setHologramState(e){if(this.state=e,!this.coreMesh)return;let t=this.coreMesh.material;e==="thinking"?t.color.setHex(16758784):e==="executing"?t.color.setHex(65437):e==="listening"?t.color.setHex(16724838):t.color.setHex(61695)}static{this.\u0275fac=function(t){return new(t||n)(Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-hologram-viewport"]],viewQuery:function(t,i){if(t&1&&Zi(UN,5),t&2){let r;Ji(r=Ki())&&(i.canvasRef=r.first)}},hostBindings:function(t,i){t&1&&Ie("resize",function(){return i.onWindowResize()},!1,jo)("mousemove",function(s){return i.onMouseMove(s)},!1,jo)},decls:12,vars:0,consts:[["hologramCanvas",""],[1,"hologram-stage"],["id","hologramCanvas"],[1,"hologram-overlay"],[1,"hologram-label"],[1,"hologram-voice-waves"],[1,"voice-bar"]],template:function(t,i){t&1&&(V(0,"div",1),ve(1,"canvas",2,0),V(3,"div",3)(4,"div",4),ee(5,"NEURAL CORE AVATAR // LIVE"),G(),V(6,"div",5),ve(7,"div",6)(8,"div",6)(9,"div",6)(10,"div",6)(11,"div",6),G()()())},dependencies:[Rt],styles:[".hologram-stage[_ngcontent-%COMP%]{position:relative;border-radius:8px;border:1px solid var(--border-cyan);background:radial-gradient(circle at center,#0064b440,#050a14d9 75%);overflow:hidden;display:flex;align-items:center;justify-content:center;width:100%;height:100%;min-height:220px}#hologramCanvas[_ngcontent-%COMP%]{width:100%;height:100%;position:absolute;top:0;left:0}.hologram-overlay[_ngcontent-%COMP%]{position:absolute;bottom:8px;left:12px;right:12px;display:flex;align-items:center;justify-content:space-between;pointer-events:none}.hologram-label[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:11px;color:var(--neon-cyan);letter-spacing:2px;text-shadow:0 0 8px var(--neon-cyan)}.hologram-voice-waves[_ngcontent-%COMP%]{display:flex;gap:3px;align-items:center;height:20px}.voice-bar[_ngcontent-%COMP%]{width:3px;height:6px;background:var(--neon-cyan);border-radius:2px;animation:_ngcontent-%COMP%_wave 1.2s infinite ease-in-out}.voice-bar[_ngcontent-%COMP%]:nth-child(2){animation-delay:.15s}.voice-bar[_ngcontent-%COMP%]:nth-child(3){animation-delay:.3s}.voice-bar[_ngcontent-%COMP%]:nth-child(4){animation-delay:.45s}.voice-bar[_ngcontent-%COMP%]:nth-child(5){animation-delay:.6s}@keyframes _ngcontent-%COMP%_wave{0%,to{height:4px}50%{height:18px}}"]})}};var Rx=(()=>{class n{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,i){this._renderer=t,this._elementRef=i}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(i){return new(i||n)(Se($o),Se(Mi))};static \u0275dir=gn({type:n})}return n})(),Nx=(()=>{class n extends Rx{static \u0275fac=(()=>{let t;return function(r){return(t||(t=Nf(n)))(r||n)}})();static \u0275dir=gn({type:n,features:[Si]})}return n})(),wm=new Ne("");var VN={provide:wm,useExisting:Lr(()=>ui),multi:!0};function BN(){let n=tr()?tr().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var HN=new Ne(""),ui=(()=>{class n extends Rx{_compositionMode;_composing=!1;constructor(t,i,r){super(t,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!BN())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(i){return new(i||n)(Se($o),Se(Mi),Se(HN,8))};static \u0275dir=gn({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&Ie("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},standalone:!1,features:[Xo([VN]),Si]})}return n})();var Px=new Ne(""),Ox=new Ne("");function Fx(n){return n!=null}function Lx(n){return qo(n)?Er(n):n}function kx(n){let e={};return n.forEach(t=>{e=t!=null?dt(dt({},e),t):e}),Object.keys(e).length===0?null:e}function Ux(n,e){return e.map(t=>t(n))}function zN(n){return!n.validate}function Vx(n){return n.map(e=>zN(e)?e:t=>e.validate(t))}function GN(n){if(!n)return null;let e=n.filter(Fx);return e.length==0?null:function(t){return kx(Ux(t,e))}}function Cm(n){return n!=null?GN(Vx(n)):null}function WN(n){if(!n)return null;let e=n.filter(Fx);return e.length==0?null:function(t){let i=Ux(t,e).map(Lx);return rd(i).pipe(hn(kx))}}function Tm(n){return n!=null?WN(Vx(n)):null}function Sx(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function jN(n){return n._rawValidators}function $N(n){return n._rawAsyncValidators}function Mm(n){return n?Array.isArray(n)?n:[n]:[]}function mu(n,e){return Array.isArray(n)?n.includes(e):n===e}function wx(n,e){let t=Mm(e);return Mm(n).forEach(r=>{mu(t,r)||t.push(r)}),t}function Cx(n,e){return Mm(e).filter(t=>!mu(n,t))}var gu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=Cm(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=Tm(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control&&this.control.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},go=class extends gu{name;get formDirective(){return null}get path(){return null}},Aa=class extends gu{_parent=null;name=null;valueAccessor=null},vu=class{_cd;constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}},qN={"[class.ng-untouched]":"isUntouched","[class.ng-touched]":"isTouched","[class.ng-pristine]":"isPristine","[class.ng-dirty]":"isDirty","[class.ng-valid]":"isValid","[class.ng-invalid]":"isInvalid","[class.ng-pending]":"isPending"},n3=wt(dt({},qN),{"[class.ng-submitted]":"isSubmitted"}),vr=(()=>{class n extends vu{constructor(t){super(t)}static \u0275fac=function(i){return new(i||n)(Se(Aa,2))};static \u0275dir=gn({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&_t("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Si]})}return n})(),vo=(()=>{class n extends vu{constructor(t){super(t)}static \u0275fac=function(i){return new(i||n)(Se(go,10))};static \u0275dir=gn({type:n,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&_t("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[Si]})}return n})();var Sa="VALID",pu="INVALID",po="PENDING",wa="DISABLED",gr=class{},yu=class extends gr{value;source;constructor(e,t){super(),this.value=e,this.source=t}},Ta=class extends gr{pristine;source;constructor(e,t){super(),this.pristine=e,this.source=t}},Da=class extends gr{touched;source;constructor(e,t){super(),this.touched=e,this.source=t}},mo=class extends gr{status;source;constructor(e,t){super(),this.status=e,this.source=t}},Em=class extends gr{source;constructor(e){super(),this.source=e}},Sm=class extends gr{source;constructor(e){super(),this.source=e}};function Bx(n){return(bu(n)?n.validators:n)||null}function XN(n){return Array.isArray(n)?Cm(n):n||null}function Hx(n,e){return(bu(e)?e.asyncValidators:n)||null}function YN(n){return Array.isArray(n)?Tm(n):n||null}function bu(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}function ZN(n,e,t){let i=n.controls;if(!(e?Object.keys(i):i).length)throw new Te(1e3,"");if(!i[t])throw new Te(1001,"")}function JN(n,e,t){n._forEachChild((i,r)=>{if(t[r]===void 0)throw new Te(1002,"")})}var _u=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(e,t){this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return Ci(this.statusReactive)}set status(e){Ci(()=>this.statusReactive.set(e))}_status=Jo(()=>this.statusReactive());statusReactive=zo(void 0);get valid(){return this.status===Sa}get invalid(){return this.status===pu}get pending(){return this.status==po}get disabled(){return this.status===wa}get enabled(){return this.status!==wa}errors;get pristine(){return Ci(this.pristineReactive)}set pristine(e){Ci(()=>this.pristineReactive.set(e))}_pristine=Jo(()=>this.pristineReactive());pristineReactive=zo(!0);get dirty(){return!this.pristine}get touched(){return Ci(this.touchedReactive)}set touched(e){Ci(()=>this.touchedReactive.set(e))}_touched=Jo(()=>this.touchedReactive());touchedReactive=zo(!1);get untouched(){return!this.touched}_events=new dn;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(wx(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(wx(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(Cx(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(Cx(e,this._rawAsyncValidators))}hasValidator(e){return mu(this._rawValidators,e)}hasAsyncValidator(e){return mu(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsTouched(wt(dt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Da(!0,i))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new Da(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsDirty(wt(dt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Ta(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),this._parent&&!e.onlySelf&&this._parent._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new Ta(!0,i))}markAsPending(e={}){this.status=po;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new mo(this.status,t)),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.markAsPending(wt(dt({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=wa,this.errors=null,this._forEachChild(r=>{r.disable(wt(dt({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new yu(this.value,i)),this._events.next(new mo(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(wt(dt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Sa,this._forEachChild(i=>{i.enable(wt(dt({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(wt(dt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){this._parent&&!e.onlySelf&&(this._parent.updateValueAndValidity(e),e.skipPristineCheck||this._parent._updatePristine({},t),this._parent._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Sa||this.status===po)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new yu(this.value,t)),this._events.next(new mo(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.updateValueAndValidity(wt(dt({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?wa:Sa}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=po,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1};let i=Lx(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=this._hasOwnPendingAsyncValidator?.emitEvent??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i&&i.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new mo(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new Ot,this.statusChanges=new Ot}_calculateStatus(){return this._allControlsDisabled()?wa:this.errors?pu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(po)?po:this._anyControlsHaveStatus(pu)?pu:Sa}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,this._parent&&!e.onlySelf&&this._parent._updatePristine(e,t),r&&this._events.next(new Ta(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new Da(this.touched,t)),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,t)}_onDisabledChange=[];_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){bu(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){let t=this._parent&&this._parent.dirty;return!e&&!!t&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=XN(this._rawValidators)}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=YN(this._rawAsyncValidators)}},xu=class extends _u{constructor(e,t,i){super(Bx(t),Hx(i,t)),this.controls=e,this._initObservables(),this._setUpdateStrategy(t),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(e,t){return this.controls[e]?this.controls[e]:(this.controls[e]=t,t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange),t)}addControl(e,t,i={}){this.registerControl(e,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(e,t={}){this.controls[e]&&this.controls[e]._registerOnCollectionChange(()=>{}),delete this.controls[e],this.updateValueAndValidity({emitEvent:t.emitEvent}),this._onCollectionChange()}setControl(e,t,i={}){this.controls[e]&&this.controls[e]._registerOnCollectionChange(()=>{}),delete this.controls[e],t&&this.registerControl(e,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(e){return this.controls.hasOwnProperty(e)&&this.controls[e].enabled}setValue(e,t={}){JN(this,!0,e),Object.keys(e).forEach(i=>{ZN(this,!0,i),this.controls[i].setValue(e[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t)}patchValue(e,t={}){e!=null&&(Object.keys(e).forEach(i=>{let r=this.controls[i];r&&r.patchValue(e[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t))}reset(e={},t={}){this._forEachChild((i,r)=>{i.reset(e?e[r]:null,{onlySelf:!0,emitEvent:t.emitEvent})}),this._updatePristine(t,this),this._updateTouched(t,this),this.updateValueAndValidity(t)}getRawValue(){return this._reduceChildren({},(e,t,i)=>(e[i]=t.getRawValue(),e))}_syncPendingControls(){let e=this._reduceChildren(!1,(t,i)=>i._syncPendingControls()?!0:t);return e&&this.updateValueAndValidity({onlySelf:!0}),e}_forEachChild(e){Object.keys(this.controls).forEach(t=>{let i=this.controls[t];i&&e(i,t)})}_setUpControls(){this._forEachChild(e=>{e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(e){for(let[t,i]of Object.entries(this.controls))if(this.contains(t)&&e(i))return!0;return!1}_reduceValue(){let e={};return this._reduceChildren(e,(t,i,r)=>((i.enabled||this.disabled)&&(t[r]=i.value),t))}_reduceChildren(e,t){let i=e;return this._forEachChild((r,s)=>{i=t(i,r,s)}),i}_allControlsDisabled(){for(let e of Object.keys(this.controls))if(this.controls[e].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(e){return this.controls.hasOwnProperty(e)?this.controls[e]:null}};var Dm=new Ne("",{providedIn:"root",factory:()=>Am}),Am="always";function KN(n,e){return[...e.path,n]}function zx(n,e,t=Am){Gx(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),eP(n,e),nP(n,e),tP(n,e),QN(n,e)}function Tx(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function QN(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function Gx(n,e){let t=jN(n);e.validator!==null?n.setValidators(Sx(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=$N(n);e.asyncValidator!==null?n.setAsyncValidators(Sx(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();Tx(e._rawValidators,r),Tx(e._rawAsyncValidators,r)}function eP(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&Wx(n,e)})}function tP(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&Wx(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function Wx(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function nP(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function iP(n,e){n==null,Gx(n,e)}function rP(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function sP(n){return Object.getPrototypeOf(n.constructor)===Nx}function oP(n,e){n._syncPendingControls(),e.forEach(t=>{let i=t.control;i.updateOn==="submit"&&i._pendingChange&&(t.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function aP(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===ui?t=s:sP(s)?i=s:r=s}),r||i||t||null}var cP={provide:go,useExisting:Lr(()=>ls)},Ca=Promise.resolve(),ls=(()=>{class n extends go{callSetDisabledState;get submitted(){return Ci(this.submittedReactive)}_submitted=Jo(()=>this.submittedReactive());submittedReactive=zo(!1);_directives=new Set;form;ngSubmit=new Ot;options;constructor(t,i,r){super(),this.callSetDisabledState=r,this.form=new xu({},Cm(t),Tm(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(t){Ca.then(()=>{let i=this._findContainer(t.path);t.control=i.registerControl(t.name,t.control),zx(t.control,t,this.callSetDisabledState),t.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(t)})}getControl(t){return this.form.get(t.path)}removeControl(t){Ca.then(()=>{let i=this._findContainer(t.path);i&&i.removeControl(t.name),this._directives.delete(t)})}addFormGroup(t){Ca.then(()=>{let i=this._findContainer(t.path),r=new xu({});iP(r,t),i.registerControl(t.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(t){Ca.then(()=>{let i=this._findContainer(t.path);i&&i.removeControl(t.name)})}getFormGroup(t){return this.form.get(t.path)}updateModel(t,i){Ca.then(()=>{this.form.get(t.path).setValue(i)})}setValue(t){this.control.setValue(t)}onSubmit(t){return this.submittedReactive.set(!0),oP(this.form,this._directives),this.ngSubmit.emit(t),this.form._events.next(new Em(this.control)),t?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(t=void 0){this.form.reset(t),this.submittedReactive.set(!1),this.form._events.next(new Sm(this.form))}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(t){return t.pop(),t.length?this.form.get(t):this.form}static \u0275fac=function(i){return new(i||n)(Se(Px,10),Se(Ox,10),Se(Dm,8))};static \u0275dir=gn({type:n,selectors:[["form",3,"ngNoForm","",3,"formGroup",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&Ie("submit",function(o){return r.onSubmit(o)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Xo([cP]),Si]})}return n})();function Dx(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function Ax(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var lP=class extends _u{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(e=null,t,i){super(Bx(t),Hx(i,t)),this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),bu(t)&&(t.nonNullable||t.initialValueIsDefault)&&(Ax(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),this._pendingChange=!1}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){Dx(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){Dx(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){Ax(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var uP={provide:Aa,useExisting:Lr(()=>ji)},Ix=Promise.resolve(),ji=(()=>{class n extends Aa{_changeDetectorRef;callSetDisabledState;control=new lP;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Ot;constructor(t,i,r,s,o,a){super(),this._changeDetectorRef=o,this.callSetDisabledState=a,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=aP(this,s)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),rP(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective&&this.formDirective.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){zx(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){Ix.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&E_(i);Ix.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?KN(t,this._parent):[t]}static \u0275fac=function(i){return new(i||n)(Se(go,9),Se(Px,10),Se(Ox,10),Se(wm,10),Se(ih,8),Se(Dm,8))};static \u0275dir=gn({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Xo([uP]),Si,bf]})}return n})();var yo=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=gn({type:n,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return n})();var dP={provide:wm,useExisting:Lr(()=>Im),multi:!0},Im=(()=>{class n extends Nx{writeValue(t){this.setProperty("value",parseFloat(t))}registerOnChange(t){this.onChange=i=>{t(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=Nf(n)))(r||n)}})();static \u0275dir=gn({type:n,selectors:[["input","type","range","formControlName",""],["input","type","range","formControl",""],["input","type","range","ngModel",""]],hostBindings:function(i,r){i&1&&Ie("change",function(o){return r.onChange(o.target.value)})("input",function(o){return r.onChange(o.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Xo([dP]),Si]})}return n})();var fP=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Vr({type:n});static \u0275inj=kr({})}return n})();var yr=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:Dm,useValue:t.callSetDisabledState??Am}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=Vr({type:n});static \u0275inj=kr({imports:[fP]})}return n})();var Eu=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.telemetry=null;this.appLaunched=new Ot;this.volumeLevel=70;this.brightnessLevel=80}onVolumeChange(e){this.volumeLevel=e,this.apiService.setVolume(e).subscribe()}onBrightnessChange(e){this.brightnessLevel=e,this.apiService.setBrightness(e).subscribe()}launchApp(e){this.audioService.playSciFiTone("ack"),this.apiService.launchApp(e).subscribe(),this.appLaunched.emit(e)}static{this.\u0275fac=function(t){return new(t||n)(Se(Qt),Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-system-diagnostics"]],inputs:{telemetry:"telemetry"},outputs:{appLaunched:"appLaunched"},decls:80,vars:13,consts:[[1,"cyber-panel"],[1,"panel-header"],[1,"fas","fa-tachometer-alt"],[2,"font-size","10px","color","var(--neon-green)"],[1,"panel-body"],[1,"metric-grid"],[1,"metric-card"],[1,"metric-title"],[1,"fas","fa-microchip"],[1,"metric-val"],[1,"metric-progress"],[1,"progress-bar"],[1,"fas","fa-memory"],[1,"progress-bar","gold"],[1,"storage-pool-box"],[1,"storage-pool-header"],[1,"fas","fa-hard-drive"],[1,"metric-progress",2,"height","8px"],[1,"progress-bar","green"],[1,"metric-val",2,"font-size","14px"],[1,"metric-val",2,"font-size","12px"],[1,"control-slider-group"],[1,"slider-item"],[1,"fas","fa-volume-high"],["type","range","min","0","max","100",3,"ngModelChange","ngModel"],[1,"fas","fa-sun"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)","letter-spacing","1px","margin-top","4px"],[1,"fas","fa-rocket"],[1,"app-grid"],[1,"btn-app",3,"click"],[1,"fab","fa-chrome"],[1,"fas","fa-code"],[1,"fas","fa-terminal"],[1,"fas","fa-sticky-note"],[1,"fas","fa-calculator"],[1,"fas","fa-folder-open"]],template:function(t,i){t&1&&(V(0,"section",0)(1,"div",1)(2,"span"),ve(3,"i",2),ee(4," System Diagnostics"),G(),V(5,"span",3),ee(6,"HOST TELEMETRY"),G()(),V(7,"div",4)(8,"div",5)(9,"div",6)(10,"div",7)(11,"span"),ee(12,"CPU Load"),G(),ve(13,"i",8),G(),V(14,"div",9),ee(15),G(),V(16,"div",10),ve(17,"div",11),G()(),V(18,"div",6)(19,"div",7)(20,"span"),ee(21,"RAM Memory"),G(),ve(22,"i",12),G(),V(23,"div",9),ee(24),G(),V(25,"div",10),ve(26,"div",13),G()()(),V(27,"div",14)(28,"div",15)(29,"span"),ve(30,"i",16),ee(31," 50GB Storage Pool"),G(),V(32,"span"),ee(33),G()(),V(34,"div",17),ve(35,"div",18),G()(),V(36,"div",5)(37,"div",6)(38,"div",7),ee(39,"Battery Status"),G(),V(40,"div",19),ee(41),G()(),V(42,"div",6)(43,"div",7),ee(44,"Host Link"),G(),V(45,"div",20),ee(46),G()()(),V(47,"div",21)(48,"div",22)(49,"label"),ve(50,"i",23),ee(51," Volume"),G(),V(52,"input",24),Ie("ngModelChange",function(s){return i.onVolumeChange(s)}),G()(),V(53,"div",22)(54,"label"),ve(55,"i",25),ee(56," Brightness"),G(),V(57,"input",24),Ie("ngModelChange",function(s){return i.onBrightnessChange(s)}),G()()(),V(58,"div",26),ve(59,"i",27),ee(60," QUICK APP LAUNCH "),G(),V(61,"div",28)(62,"button",29),Ie("click",function(){return i.launchApp("chrome")}),ve(63,"i",30),ee(64," Chrome"),G(),V(65,"button",29),Ie("click",function(){return i.launchApp("vscode")}),ve(66,"i",31),ee(67," VS Code"),G(),V(68,"button",29),Ie("click",function(){return i.launchApp("terminal")}),ve(69,"i",32),ee(70," Terminal"),G(),V(71,"button",29),Ie("click",function(){return i.launchApp("notepad")}),ve(72,"i",33),ee(73," Notepad"),G(),V(74,"button",29),Ie("click",function(){return i.launchApp("calc")}),ve(75,"i",34),ee(76," Calc"),G(),V(77,"button",29),Ie("click",function(){return i.launchApp("explorer")}),ve(78,"i",35),ee(79," Explorer"),G()()()()),t&2&&(ne(15),Ct("",(i.telemetry==null||i.telemetry.cpu==null?null:i.telemetry.cpu.percent)||0,"%"),ne(2),wi("width",(i.telemetry==null||i.telemetry.cpu==null?null:i.telemetry.cpu.percent)||0,"%"),ne(7),Ct("",(i.telemetry==null||i.telemetry.memory==null?null:i.telemetry.memory.percent)||0,"%"),ne(2),wi("width",(i.telemetry==null||i.telemetry.memory==null?null:i.telemetry.memory.percent)||0,"%"),ne(7),Ct("",(i.telemetry==null||i.telemetry.storage_pool_50gb==null?null:i.telemetry.storage_pool_50gb.used_mb)||0," MB / 50 GB"),ne(2),wi("width",(i.telemetry==null||i.telemetry.storage_pool_50gb==null?null:i.telemetry.storage_pool_50gb.used_percentage)||0,"%"),ne(6),Ct(" ",i.telemetry!=null&&i.telemetry.battery?i.telemetry.battery.percent+"% "+(i.telemetry.battery.power_plugged?"(AC)":"(BAT)"):"AC Connected"," "),ne(5),Ct(" ",(i.telemetry==null||i.telemetry.os==null?null:i.telemetry.os.hostname)||"Win32 Host"," "),ne(6),Ye("ngModel",i.volumeLevel),ne(5),Ye("ngModel",i.brightnessLevel))},dependencies:[Rt,yr,ui,Im,vr,ji],styles:["[_nghost-%COMP%]{display:block;height:100%}.cyber-panel[_ngcontent-%COMP%]{background:var(--bg-panel);border:1px solid var(--border-cyan);border-radius:8px;display:flex;flex-direction:column;backdrop-filter:blur(14px);box-shadow:0 4px 20px #00000080;position:relative;overflow:hidden;height:100%}.panel-header[_ngcontent-%COMP%]{height:38px;background:#00f0ff14;border-bottom:1px solid var(--border-cyan);display:flex;align-items:center;justify-content:space-between;padding:0 12px;font-family:var(--font-hud);font-size:12px;letter-spacing:1.5px;color:var(--neon-cyan);text-transform:uppercase;flex-shrink:0}.panel-body[_ngcontent-%COMP%]{flex:1;padding:12px;overflow-y:auto;display:flex;flex-direction:column;gap:10px}.metric-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr 1fr;gap:8px}.metric-card[_ngcontent-%COMP%]{background:var(--bg-panel-light);border:1px solid rgba(0,240,255,.2);border-radius:6px;padding:8px 10px}.metric-title[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;display:flex;justify-content:space-between}.metric-val[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:18px;color:var(--neon-cyan);margin:4px 0}.metric-progress[_ngcontent-%COMP%]{height:5px;background:#ffffff1a;border-radius:3px;overflow:hidden}.progress-bar[_ngcontent-%COMP%]{height:100%;background:linear-gradient(90deg,var(--neon-blue),var(--neon-cyan));width:0%;transition:width .5s ease}.progress-bar.gold[_ngcontent-%COMP%]{background:linear-gradient(90deg,var(--neon-gold),#ff8800)}.progress-bar.green[_ngcontent-%COMP%]{background:linear-gradient(90deg,#00b060,var(--neon-green))}.storage-pool-box[_ngcontent-%COMP%]{background:#00f0ff0d;border:1px solid rgba(0,240,255,.3);border-radius:6px;padding:8px 10px}.storage-pool-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;font-family:var(--font-data);font-size:12px;color:var(--neon-cyan);margin-bottom:6px}.control-slider-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:8px}.slider-item[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px;font-family:var(--font-data);font-size:13px}.slider-item[_ngcontent-%COMP%]   label[_ngcontent-%COMP%]{width:75px;color:var(--text-dim)}.slider-item[_ngcontent-%COMP%]   input[type=range][_ngcontent-%COMP%]{flex:1;accent-color:var(--neon-cyan);cursor:pointer}.app-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}.btn-app[_ngcontent-%COMP%]{background:#00f0ff1a;border:1px solid rgba(0,240,255,.25);border-radius:4px;color:#fff;padding:8px 4px;font-size:11px;font-family:var(--font-data);display:flex;flex-direction:column;align-items:center;gap:4px;cursor:pointer;transition:all .2s}.btn-app[_ngcontent-%COMP%]:hover{background:#00f0ff40;border-color:var(--neon-cyan);transform:translateY(-1px)}"]})}};var hP=["chatScroll"];function pP(n,e){if(n&1&&(V(0,"div",17),ve(1,"img",18),G()),n&2){let t=qt().$implicit;ne(),Ye("src",t.imagePreview,Py)}}function mP(n,e){if(n&1&&(V(0,"div",19),ve(1,"i",20),ee(2),G()),n&2){let t=qt().$implicit;ne(2),Ct(" Executed: ",t.toolCall.tool," ")}}function gP(n,e){if(n&1&&(V(0,"div",13)(1,"div",14),ee(2),Yo(3,"uppercase"),G(),V(4,"div"),ee(5),G(),kt(6,pP,2,1,"div",15)(7,mP,3,1,"div",16),G()),n&2){let t=e.$implicit;Ye("ngClass",t.role==="user"?"user":"jarvis"),ne(2),Xt(Zo(3,5,t.sender)),ne(3),Xt(t.content),ne(),Ye("ngIf",t.imagePreview),ne(),Ye("ngIf",t.toolCall)}}var Ia=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.selectedAgent="supervisor";this.agentResponse=new Ot;this.messages=[{sender:"JARVIS",role:"assistant",content:"Online and ready, sir. All systems, 50GB storage vault, and OS controllers are active."}];this.inputText="";this.subs=[];this.subs.push(this.audioService.speechResult$.subscribe(i=>{this.inputText=i,this.sendMessage()}))}ngAfterViewChecked(){this.scrollToBottom()}toggleMic(){this.audioService.isListening?this.audioService.stopListening():this.audioService.startListening()}sendMessage(){let e=this.inputText.trim();e&&(this.audioService.playSciFiTone("beep"),this.messages.push({sender:"User",role:"user",content:e}),this.inputText="",this.audioService.avatarState$.next("thinking"),this.apiService.chatWithJarvis(e,this.selectedAgent).subscribe({next:t=>{this.audioService.avatarState$.next("idle");let i=t.response||"Task executed successfully, sir.";this.messages.push({sender:"JARVIS",role:"assistant",content:i,toolCall:t.tool_call}),this.audioService.speak(i),this.agentResponse.emit(t)},error:t=>{this.audioService.avatarState$.next("idle"),this.audioService.playSciFiTone("error"),this.messages.push({sender:"JARVIS",role:"assistant",content:`An anomaly occurred: ${t.message}`})}}))}addSystemMessage(e,t,i=null){this.messages.push({sender:e,role:"assistant",content:t,imagePreview:i})}scrollToBottom(){try{this.chatScrollContainer&&(this.chatScrollContainer.nativeElement.scrollTop=this.chatScrollContainer.nativeElement.scrollHeight)}catch{}}static{this.\u0275fac=function(t){return new(t||n)(Se(Qt),Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-chat-console"]],viewQuery:function(t,i){if(t&1&&Zi(hP,5),t&2){let r;Ji(r=Ki())&&(i.chatScrollContainer=r.first)}},inputs:{selectedAgent:"selectedAgent"},outputs:{agentResponse:"agentResponse"},decls:18,vars:7,consts:[["chatScroll",""],[1,"chat-container"],[1,"panel-header"],[1,"fas","fa-comments"],[2,"color","var(--text-dim)","font-size","11px"],[1,"chat-messages"],["class","chat-bubble",3,"ngClass",4,"ngFor","ngForOf"],[1,"chat-input-bar",3,"ngSubmit"],["type","button","title","Voice Input",1,"btn-mic",3,"click"],[1,"fas","fa-microphone"],["type","text","name","inputText","placeholder","Ask Jarvis or give an OS command (e.g. 'Set volume 50', 'Open VS Code', 'Index my Documents')...","autocomplete","off",1,"chat-input",3,"ngModelChange","ngModel"],["type","submit",1,"btn-cyber"],[1,"fas","fa-paper-plane"],[1,"chat-bubble",3,"ngClass"],[1,"bubble-sender"],["style","margin-top:8px;",4,"ngIf"],["class","tool-chip",4,"ngIf"],[2,"margin-top","8px"],[2,"max-width","100%","border-radius","4px","border","1px solid var(--neon-cyan)",3,"src"],[1,"tool-chip"],[1,"fas","fa-microchip"]],template:function(t,i){if(t&1){let r=On();V(0,"div",1)(1,"div",2)(2,"span"),ve(3,"i",3),ee(4," JARVIS Command Terminal"),G(),V(5,"span",4),ee(6),Yo(7,"uppercase"),G()(),V(8,"div",5,0),kt(10,gP,8,7,"div",6),G(),V(11,"form",7),Ie("ngSubmit",function(){return At(r),It(i.sendMessage())}),V(12,"button",8),Ie("click",function(){return At(r),It(i.toggleMic())}),ve(13,"i",9),G(),V(14,"input",10),er("ngModelChange",function(o){return At(r),Br(i.inputText,o)||(i.inputText=o),It(o)}),G(),V(15,"button",11),ve(16,"i",12),ee(17," SEND"),G()()()}t&2&&(ne(6),Ct("ACTIVE AGENT: [",Zo(7,5,i.selectedAgent),"]"),ne(4),Ye("ngForOf",i.messages),ne(2),_t("recording",i.audioService.isListening),ne(2),Qi("ngModel",i.inputText))},dependencies:[Rt,Ti,Ln,Di,Qo,yr,yo,ui,vr,vo,ji,ls],styles:["[_nghost-%COMP%]{display:block;height:100%}.chat-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;background:var(--bg-panel);border:1px solid var(--border-cyan);border-radius:8px;overflow:hidden;height:100%}.panel-header[_ngcontent-%COMP%]{height:38px;background:#00f0ff14;border-bottom:1px solid var(--border-cyan);display:flex;align-items:center;justify-content:space-between;padding:0 12px;font-family:var(--font-hud);font-size:12px;letter-spacing:1.5px;color:var(--neon-cyan);text-transform:uppercase;flex-shrink:0}.chat-messages[_ngcontent-%COMP%]{flex:1;padding:12px;overflow-y:auto;display:flex;flex-direction:column;gap:10px;min-height:0}.chat-bubble[_ngcontent-%COMP%]{max-width:85%;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.45;animation:_ngcontent-%COMP%_fadeIn .25s ease;word-break:break-word}.chat-bubble.user[_ngcontent-%COMP%]{align-self:flex-end;background:#0088ff40;border:1px solid var(--neon-blue);color:#fff}.chat-bubble.jarvis[_ngcontent-%COMP%]{align-self:flex-start;background:#00f0ff1f;border:1px solid var(--neon-cyan);color:#e6f7ff}.bubble-sender[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:10px;color:var(--neon-cyan);margin-bottom:4px;letter-spacing:1px}.tool-chip[_ngcontent-%COMP%]{display:inline-flex;align-items:center;gap:6px;background:#ffb80026;border:1px solid var(--neon-gold);color:var(--neon-gold);border-radius:4px;padding:3px 8px;font-family:var(--font-data);font-size:11px;margin-top:6px}.chat-input-bar[_ngcontent-%COMP%]{display:flex;padding:8px 12px;background:#050c19cc;border-top:1px solid var(--border-cyan);gap:8px;align-items:center;flex-shrink:0}.chat-input[_ngcontent-%COMP%]{flex:1;background:#00f0ff0d;border:1px solid var(--border-cyan);border-radius:6px;padding:8px 12px;color:#fff;font-family:var(--font-body);font-size:13px;outline:none;transition:border-color .2s}.chat-input[_ngcontent-%COMP%]:focus{border-color:var(--neon-cyan);box-shadow:0 0 8px #00f0ff4d}.btn-mic[_ngcontent-%COMP%]{width:38px;height:38px;border-radius:50%;background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);display:flex;align-items:center;justify-content:center;cursor:pointer;transition:all .2s ease;flex-shrink:0}.btn-mic.recording[_ngcontent-%COMP%]{background:var(--neon-red);color:#fff;border-color:#f68;animation:_ngcontent-%COMP%_pulse-red 1s infinite}.btn-cyber[_ngcontent-%COMP%]{background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);font-family:var(--font-hud);font-size:11px;letter-spacing:1px;padding:6px 12px;border-radius:4px;cursor:pointer;transition:all .2s ease;display:inline-flex;align-items:center;gap:6px}.btn-cyber[_ngcontent-%COMP%]:hover{background:var(--neon-cyan);color:#000;box-shadow:0 0 12px var(--neon-cyan)}@keyframes _ngcontent-%COMP%_fadeIn{0%{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}@keyframes _ngcontent-%COMP%_pulse-red{0%,to{transform:scale(1);box-shadow:0 0 4px var(--neon-red)}50%{transform:scale(1.08);box-shadow:0 0 14px var(--neon-red)}}"]})}};function vP(n,e){n&1&&ve(0,"i",13)}function yP(n,e){if(n&1&&(V(0,"span",16),ee(1),G()),n&2){let t=e.$implicit;ne(),Xt(t)}}function _P(n,e){if(n&1&&(V(0,"div",14),kt(1,yP,2,1,"span",15),G()),n&2){let t=qt().$implicit;ne(),Ye("ngForOf",t.capabilities)}}function xP(n,e){if(n&1){let t=On();V(0,"div",6),Ie("click",function(){let r=At(t).$implicit,s=qt();return It(s.selectAgent(r.id))}),V(1,"div",7)(2,"div",8),kt(3,vP,1,0,"i",9),ee(4),G(),V(5,"div",10),ee(6),Yo(7,"uppercase"),G()(),V(8,"div",11),ee(9),G(),kt(10,_P,2,1,"div",12),G()}if(n&2){let t=e.$implicit,i=qt();_t("selected",i.selectedAgentId===t.id),ne(2),wi("color",t.avatar_color||"#fff"),ne(),Ye("ngIf",i.selectedAgentId===t.id),ne(),Ct(" ",t.name," "),ne(),Ye("ngClass",t.status),ne(),Xt(Zo(7,10,t.status)),ne(3),Xt(t.role),ne(),Ye("ngIf",t.capabilities)}}var Su=class n{constructor(e){this.audioService=e;this.agents=[];this.selectedAgentId="supervisor";this.agentSelected=new Ot}selectAgent(e){this.audioService.playSciFiTone("ack"),this.agentSelected.emit(e)}static{this.\u0275fac=function(t){return new(t||n)(Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-multi-agent-swarm"]],inputs:{agents:"agents",selectedAgentId:"selectedAgentId"},outputs:{agentSelected:"agentSelected"},decls:9,vars:1,consts:[[1,"agent-swarm-panel"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)","margin-bottom","8px","letter-spacing","1px","display","flex","justify-content","space-between","align-items","center"],[1,"fas","fa-users-cog"],[2,"font-size","10px","color","var(--text-dim)"],[1,"agent-list"],["class","agent-card",3,"selected","click",4,"ngFor","ngForOf"],[1,"agent-card",3,"click"],[1,"agent-card-header"],[1,"agent-name"],["class","fas fa-check-circle","style","color:var(--neon-cyan); margin-right:4px;",4,"ngIf"],[1,"agent-status-tag",3,"ngClass"],[1,"agent-role"],["class","capabilities-list",4,"ngIf"],[1,"fas","fa-check-circle",2,"color","var(--neon-cyan)","margin-right","4px"],[1,"capabilities-list"],["class","cap-pill",4,"ngFor","ngForOf"],[1,"cap-pill"]],template:function(t,i){t&1&&(V(0,"div",0)(1,"div",1)(2,"span"),ve(3,"i",2),ee(4," MULTI-AGENT SWARM"),G(),V(5,"span",3),ee(6,"CLICK TO SELECT"),G()(),V(7,"div",4),kt(8,xP,11,12,"div",5),G()()),t&2&&(ne(8),Ye("ngForOf",i.agents))},dependencies:[Rt,Ti,Ln,Di,Qo],styles:[".agent-swarm-panel[_ngcontent-%COMP%], .agent-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:8px}.agent-card[_ngcontent-%COMP%]{background:var(--bg-panel-light);border:1px solid rgba(0,240,255,.2);border-radius:6px;padding:8px 10px;display:flex;flex-direction:column;gap:6px;cursor:pointer;transition:all .2s ease}.agent-card[_ngcontent-%COMP%]:hover{border-color:var(--neon-cyan);background:#00f0ff14}.agent-card.selected[_ngcontent-%COMP%]{border-color:var(--neon-cyan);background:#00f0ff24;box-shadow:0 0 10px #00f0ff33}.agent-card-header[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between}.agent-name[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:11px}.agent-role[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:11px;color:var(--text-dim)}.agent-status-tag[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:10px;padding:2px 6px;border-radius:4px;background:#00ff9d1a;border:1px solid var(--neon-green);color:var(--neon-green)}.agent-status-tag.thinking[_ngcontent-%COMP%]{background:#ffb80026;border-color:var(--neon-gold);color:var(--neon-gold)}.agent-status-tag.executing[_ngcontent-%COMP%]{background:#00f0ff33;border-color:var(--neon-cyan);color:var(--neon-cyan)}.capabilities-list[_ngcontent-%COMP%]{display:flex;gap:4px;flex-wrap:wrap}.cap-pill[_ngcontent-%COMP%]{font-size:10px;color:var(--text-dim);background:#ffffff0d;padding:1px 4px;border-radius:3px}"]})}};function bP(n,e){if(n&1&&(V(0,"div",19),ee(1),G()),n&2){let t=qt();ne(),Ct(" ",t.statusMessage," ")}}function MP(n,e){if(n&1&&(V(0,"div",22)(1,"div",23),ee(2),G(),V(3,"div",24),ee(4),G(),V(5,"div",25),ee(6),G()()),n&2){let t=e.$implicit;ne(2),th("",t.file_name," (",t.category,")"),ne(2),Xt(t.file_path),ne(2),Xt(t.summary||"Indexed document")}}function EP(n,e){if(n&1&&(V(0,"div",20),kt(1,MP,7,4,"div",21),G()),n&2){let t=qt();ne(),Ye("ngForOf",t.searchResults)}}function SP(n,e){n&1&&(V(0,"div",26),ee(1," No files currently in 50GB storage pool. "),G())}function wP(n,e){if(n&1&&(V(0,"div",27)(1,"div"),ve(2,"i",28),V(3,"span"),ee(4),G()(),V(5,"div",29),ee(6),G()()),n&2){let t=e.$implicit;ne(2),Ye("ngClass",t.is_directory?"fa-folder":"fa-file-code"),ne(2),Xt(t.name),ne(2),Ct(" ",t.size_formatted," ")}}var wu=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.files=[];this.searchResults=[];this.searchQuery="";this.indexDirPath="";this.statusMessage=""}ngOnInit(){this.loadFiles()}loadFiles(){this.apiService.listStorageFiles().subscribe(e=>{this.files=e.files||[]})}onFileSelected(e){let t=e.target.files[0];t&&(this.statusMessage=`Uploading ${t.name}...`,this.apiService.uploadStorageFile(t).subscribe({next:i=>{this.audioService.playSciFiTone("ack"),this.statusMessage=`Saved ${i.filename} (${i.size} bytes).`,this.loadFiles()},error:i=>{this.audioService.playSciFiTone("error"),this.statusMessage=`Upload error: ${i.message}`}}))}searchKnowledge(){let e=this.searchQuery.trim();e&&(this.audioService.playSciFiTone("beep"),this.apiService.searchKnowledge(e).subscribe(t=>{this.searchResults=t.results||[]}))}indexDirectory(){let e=this.indexDirPath.trim();e&&(this.audioService.playSciFiTone("ack"),this.statusMessage=`Scanning and indexing ${e}...`,this.apiService.indexDirectory(e).subscribe({next:t=>{this.statusMessage=`Indexed ${t.indexed_count} files into SQLite knowledge base.`,this.loadFiles()},error:t=>{this.statusMessage=`Index error: ${t.message}`}}))}static{this.\u0275fac=function(t){return new(t||n)(Se(Qt),Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-storage-vault"]],decls:29,vars:6,consts:[["fileInput",""],[1,"storage-vault-container"],[1,"dropzone",3,"click"],[1,"fas","fa-cloud-arrow-up",2,"font-size","18px","color","var(--neon-cyan)","margin-bottom","4px"],["type","file",2,"display","none",3,"change"],["class","status-alert",4,"ngIf"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)","margin","6px 0"],[1,"fas","fa-search"],[1,"search-form",3,"ngSubmit"],["type","text","name","searchQuery","placeholder","Search indexed files...",1,"chat-input",3,"ngModelChange","ngModel"],["type","submit",1,"btn-cyber"],["class","search-results-list",4,"ngIf"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)","margin","8px 0 4px 0"],[1,"fas","fa-folder-plus"],["type","text","name","indexDirPath","placeholder","Path (e.g. C:\\Users\\AYYAPPA RAYUDU\\Documents)",1,"chat-input",3,"ngModelChange","ngModel"],[1,"fas","fa-server"],[1,"file-list"],["style","color:var(--text-dim); font-size:11px; padding:6px;",4,"ngIf"],["class","file-item",4,"ngFor","ngForOf"],[1,"status-alert"],[1,"search-results-list"],["class","search-item",4,"ngFor","ngForOf"],[1,"search-item"],[1,"search-name"],[1,"search-path"],[1,"search-summary"],[2,"color","var(--text-dim)","font-size","11px","padding","6px"],[1,"file-item"],[1,"fas",2,"color","var(--neon-cyan)","margin-right","6px",3,"ngClass"],[2,"color","var(--text-dim)","font-size","11px"]],template:function(t,i){if(t&1){let r=On();V(0,"div",1)(1,"div",2),Ie("click",function(){At(r);let o=__(6);return It(o.click())}),ve(2,"i",3),V(3,"div"),ee(4,"Click or drop file to upload to 50GB Pool"),G(),V(5,"input",4,0),Ie("change",function(o){return At(r),It(i.onFileSelected(o))}),G()(),kt(7,bP,2,1,"div",5),V(8,"div",6),ve(9,"i",7),ee(10," SEARCH LAPTOP KNOWLEDGE "),G(),V(11,"form",8),Ie("ngSubmit",function(){return At(r),It(i.searchKnowledge())}),V(12,"input",9),er("ngModelChange",function(o){return At(r),Br(i.searchQuery,o)||(i.searchQuery=o),It(o)}),G(),V(13,"button",10),ve(14,"i",7),G()(),kt(15,EP,2,1,"div",11),V(16,"div",12),ve(17,"i",13),ee(18," DEEP DIRECTORY INDEXER "),G(),V(19,"form",8),Ie("ngSubmit",function(){return At(r),It(i.indexDirectory())}),V(20,"input",14),er("ngModelChange",function(o){return At(r),Br(i.indexDirPath,o)||(i.indexDirPath=o),It(o)}),G(),V(21,"button",10),ee(22,"INDEX"),G()(),V(23,"div",12),ve(24,"i",15),ee(25," 50GB POOL EXPLORER "),G(),V(26,"div",16),kt(27,SP,2,0,"div",17)(28,wP,7,3,"div",18),G()()}t&2&&(ne(7),Ye("ngIf",i.statusMessage),ne(5),Qi("ngModel",i.searchQuery),ne(3),Ye("ngIf",i.searchResults.length>0),ne(5),Qi("ngModel",i.indexDirPath),ne(7),Ye("ngIf",i.files.length===0),ne(),Ye("ngForOf",i.files))},dependencies:[Rt,Ti,Ln,Di,yr,yo,ui,vr,vo,ji,ls],styles:[".storage-vault-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:6px}.dropzone[_ngcontent-%COMP%]{border:1px dashed var(--border-cyan);border-radius:6px;padding:10px;text-align:center;background:#00f0ff08;cursor:pointer;transition:all .2s;font-family:var(--font-data);font-size:12px;color:var(--text-dim)}.dropzone[_ngcontent-%COMP%]:hover{background:#00f0ff14;border-color:var(--neon-cyan);color:#fff}.status-alert[_ngcontent-%COMP%]{background:#00ff9d1a;border:1px solid var(--neon-green);border-radius:4px;padding:4px 8px;font-family:var(--font-data);font-size:11px;color:var(--neon-green)}.search-form[_ngcontent-%COMP%]{display:flex;gap:6px;margin-bottom:6px}.chat-input[_ngcontent-%COMP%]{flex:1;background:#00f0ff0d;border:1px solid var(--border-cyan);border-radius:6px;padding:6px 10px;color:#fff;font-family:var(--font-body);font-size:12px;outline:none}.chat-input[_ngcontent-%COMP%]:focus{border-color:var(--neon-cyan)}.search-results-list[_ngcontent-%COMP%]{max-height:110px;overflow-y:auto;display:flex;flex-direction:column;gap:6px;margin-bottom:8px}.search-item[_ngcontent-%COMP%]{background:#00f0ff0f;border:1px solid rgba(0,240,255,.2);border-radius:4px;padding:6px}.search-name[_ngcontent-%COMP%]{color:var(--neon-cyan);font-weight:700;font-size:12px}.search-path[_ngcontent-%COMP%]{color:var(--text-dim);font-size:10px}.search-summary[_ngcontent-%COMP%]{font-size:11px;color:#cde3f8;margin-top:2px}.file-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:4px;max-height:140px;overflow-y:auto}.file-item[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;padding:6px 8px;background:#00f0ff0a;border:1px solid rgba(0,240,255,.1);border-radius:4px;font-family:var(--font-data);font-size:12px}.file-item[_ngcontent-%COMP%]:hover{background:#00f0ff1a}.btn-cyber[_ngcontent-%COMP%]{background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);font-family:var(--font-hud);font-size:11px;padding:6px 10px;border-radius:4px;cursor:pointer}.btn-cyber[_ngcontent-%COMP%]:hover{background:var(--neon-cyan);color:#000}"]})}};function CP(n,e){if(n&1){let t=On();V(0,"tr")(1,"td"),ee(2),G(),V(3,"td",10),ee(4),G(),V(5,"td",11),ee(6),G(),V(7,"td",7),ee(8),G(),V(9,"td",8)(10,"button",12),Ie("click",function(){let r=At(t).$implicit,s=qt();return It(s.kill(r.pid))}),ee(11,"KILL"),G()()()}if(n&2){let t=e.$implicit;ne(2),Xt(t.pid),ne(),Ye("title",t.name),ne(),Xt(t.name),ne(2),Ct("",t.cpu_percent,"%"),ne(2),Ct("",t.memory_percent,"%")}}var Cu=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.processes=[]}ngOnInit(){this.refresh()}refresh(){this.apiService.getProcesses(25).subscribe(e=>{this.processes=e.processes||[]})}kill(e){confirm(`Terminate process PID ${e}?`)&&(this.audioService.playSciFiTone("ack"),this.apiService.killProcess(e).subscribe(()=>this.refresh()))}static{this.\u0275fac=function(t){return new(t||n)(Se(Qt),Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-process-manager"]],decls:23,vars:1,consts:[[1,"process-manager-container"],[1,"process-header"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)"],[1,"btn-cyber",2,"padding","2px 8px","font-size","10px",3,"click"],[1,"fas","fa-sync"],[1,"process-table-scroll"],[1,"proc-table"],[2,"text-align","right"],[2,"text-align","center"],[4,"ngFor","ngForOf"],[1,"proc-name",3,"title"],[2,"text-align","right","color","var(--neon-gold)"],[1,"btn-cyber","danger",2,"padding","2px 6px","font-size","9px",3,"click"]],template:function(t,i){t&1&&(V(0,"div",0)(1,"div",1)(2,"span",2),ee(3,"ACTIVE PROCESSES"),G(),V(4,"button",3),Ie("click",function(){return i.refresh()}),ve(5,"i",4),ee(6," REFRESH "),G()(),V(7,"div",5)(8,"table",6)(9,"thead")(10,"tr")(11,"th"),ee(12,"PID"),G(),V(13,"th"),ee(14,"NAME"),G(),V(15,"th",7),ee(16,"CPU%"),G(),V(17,"th",7),ee(18,"MEM%"),G(),V(19,"th",8),ee(20,"ACTION"),G()()(),V(21,"tbody"),kt(22,CP,12,5,"tr",9),G()()()()),t&2&&(ne(22),Ye("ngForOf",i.processes))},dependencies:[Rt,Ln],styles:[".process-manager-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:8px}.process-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center}.process-table-scroll[_ngcontent-%COMP%]{max-height:380px;overflow-y:auto}.proc-table[_ngcontent-%COMP%]{width:100%;border-collapse:collapse;font-size:11px;font-family:var(--font-data)}.proc-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]{text-align:left;padding:4px;color:var(--neon-cyan);border-bottom:1px solid rgba(0,240,255,.2)}.proc-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]{padding:4px;border-bottom:1px solid rgba(255,255,255,.05)}.proc-name[_ngcontent-%COMP%]{max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.btn-cyber[_ngcontent-%COMP%]{background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);font-family:var(--font-hud);border-radius:4px;cursor:pointer}.btn-cyber.danger[_ngcontent-%COMP%]{border-color:var(--neon-red);color:var(--neon-red);background:#ff336626}.btn-cyber.danger[_ngcontent-%COMP%]:hover{background:var(--neon-red);color:#fff}"]})}};var TP=["termScroll"];function DP(n,e){if(n&1&&(V(0,"div"),ee(1),G()),n&2){let t=e.$implicit;wi("color",t.isError?"var(--neon-red)":"#a8d5ff"),ne(),Ct(" ",t.text," ")}}var Tu=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.commandInput="";this.terminalLines=[{text:"JARVIS PowerShell Direct Console [Win32 Native Engine]"},{text:"Type any PowerShell command and hit Enter..."}]}execute(){let e=this.commandInput.trim();e&&(this.audioService.playSciFiTone("beep"),this.terminalLines.push({text:`PS > ${e}`}),this.commandInput="",this.audioService.avatarState$.next("executing"),this.apiService.executeTerminal(e).subscribe({next:t=>{this.audioService.avatarState$.next("idle"),t.stdout&&this.terminalLines.push({text:t.stdout}),t.stderr&&this.terminalLines.push({text:`ERROR: ${t.stderr}`,isError:!0}),this.terminalLines.push({text:`[Process exit: ${t.exit_code} | ${t.duration_ms}ms]`}),this.scrollToBottom()},error:t=>{this.audioService.avatarState$.next("idle"),this.terminalLines.push({text:`EXECUTION FAILURE: ${t.message}`,isError:!0}),this.scrollToBottom()}}))}scrollToBottom(){setTimeout(()=>{this.termScrollContainer&&(this.termScrollContainer.nativeElement.scrollTop=this.termScrollContainer.nativeElement.scrollHeight)},50)}static{this.\u0275fac=function(t){return new(t||n)(Se(Qt),Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-terminal-runner"]],viewQuery:function(t,i){if(t&1&&Zi(TP,5),t&2){let r;Ji(r=Ki())&&(i.termScrollContainer=r.first)}},decls:8,vars:2,consts:[["termScroll",""],[1,"terminal-window"],[1,"terminal-output"],[3,"color",4,"ngFor","ngForOf"],[1,"terminal-input-row",3,"ngSubmit"],["type","text","name","commandInput","placeholder","Get-Process, ipconfig, ls, Get-Service...",3,"ngModelChange","ngModel"]],template:function(t,i){if(t&1){let r=On();V(0,"div",1)(1,"div",2,0),kt(3,DP,2,3,"div",3),G(),V(4,"form",4),Ie("ngSubmit",function(){return At(r),It(i.execute())}),V(5,"span"),ee(6,"PS >"),G(),V(7,"input",5),er("ngModelChange",function(o){return At(r),Br(i.commandInput,o)||(i.commandInput=o),It(o)}),G()()()}t&2&&(ne(3),Ye("ngForOf",i.terminalLines),ne(4),Qi("ngModel",i.commandInput))},dependencies:[Rt,Ln,yr,yo,ui,vr,vo,ji,ls],styles:[".terminal-window[_ngcontent-%COMP%]{background:#02050b;border:1px solid rgba(0,240,255,.25);border-radius:6px;height:250px;display:flex;flex-direction:column;font-family:Courier New,monospace;font-size:12px}.terminal-output[_ngcontent-%COMP%]{flex:1;padding:8px;overflow-y:auto;color:#a8d5ff;white-space:pre-wrap;line-height:1.35}.terminal-input-row[_ngcontent-%COMP%]{display:flex;border-top:1px solid rgba(0,240,255,.2);padding:4px 8px;background:#00f0ff0d;align-items:center;gap:6px}.terminal-input-row[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{color:var(--neon-green)}.terminal-input-row[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{flex:1;background:transparent;border:none;color:#fff;font-family:inherit;font-size:12px;outline:none}"]})}};var Du=class n{static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-hud-footer"]],decls:8,vars:0,consts:[[1,"hud-footer"],[1,"fas","fa-shield-halved",2,"color","var(--neon-green)"]],template:function(t,i){t&1&&(V(0,"footer",0)(1,"div"),ve(2,"i",1),ee(3," SECURITY PROTOCOLS: ACTIVE"),G(),V(4,"div"),ee(5,"50GB LOCAL STORAGE ALLOCATED // MEMORY PERSISTED"),G(),V(6,"div"),ee(7,"ANGULAR 19+ CORE // FASTAPI + OLLAMA QWEN3:8B"),G()())},dependencies:[Rt],styles:[".hud-footer[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;background:var(--bg-panel);border:1px solid var(--border-cyan);border-radius:6px;padding:4px 14px;font-family:var(--font-data);font-size:11px;color:var(--text-dim);flex-wrap:wrap;gap:6px}"]})}};var Au=class n{constructor(){this.ws=null;this.reconnectInterval=3e3;this.telemetrySubject=new pi(null);this.telemetry$=this.telemetrySubject.asObservable();this.agentsSubject=new pi([]);this.agents$=this.agentsSubject.asObservable();this.isConnectedSubject=new pi(!1);this.isConnected$=this.isConnectedSubject.asObservable();this.connect()}connect(){let e=window.location.protocol==="https:"?"wss:":"ws:",t=window.location.host||"localhost:8000",i=`${e}//${t}/ws/telemetry`;try{this.ws=new WebSocket(i),this.ws.onopen=()=>{console.log("[JARVIS-WS] Connected to live telemetry stream"),this.isConnectedSubject.next(!0)},this.ws.onmessage=r=>{try{let s=JSON.parse(r.data);s.type==="TELEMETRY_UPDATE"&&(s.telemetry&&this.telemetrySubject.next(s.telemetry),s.agents&&this.agentsSubject.next(s.agents))}catch(s){console.error("[JARVIS-WS] Parse error",s)}},this.ws.onclose=()=>{this.isConnectedSubject.next(!1),setTimeout(()=>this.connect(),this.reconnectInterval)},this.ws.onerror=r=>{this.isConnectedSubject.next(!1)}}catch{setTimeout(()=>this.connect(),this.reconnectInterval)}}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275prov=tt({token:n,factory:n.\u0275fac,providedIn:"root"})}};function IP(n,e){if(n&1){let t=On();V(0,"div",19)(1,"button",20),Ie("click",function(){At(t);let r=qt();return It(r.setMobileView("diagnostics"))}),ve(2,"i",21),ee(3," OS STATUS "),G(),V(4,"button",20),Ie("click",function(){At(t);let r=qt();return It(r.setMobileView("chat"))}),ve(5,"i",22),ee(6," JARVIS CORE "),G(),V(7,"button",20),Ie("click",function(){At(t);let r=qt();return It(r.setMobileView("swarm"))}),ve(8,"i",23),ee(9," SWARM & VAULT "),G()()}if(n&2){let t=qt();ne(),_t("active",t.mobileView==="diagnostics"),ne(3),_t("active",t.mobileView==="chat"),ne(3),_t("active",t.mobileView==="swarm")}}var Iu=class n{constructor(e,t,i){this.wsService=e;this.apiService=t;this.audioService=i;this.activeTab="agents";this.mobileView="chat";this.isMobile=!1;this.selectedAgentId="supervisor";this.telemetry=null;this.agents=[];this.isConnected=!1;this.netSpeedText="NET: ACTIVE";this.checkScreenSize()}onResize(){this.checkScreenSize()}checkScreenSize(){this.isMobile=window.innerWidth<992}ngOnInit(){this.wsService.isConnected$.subscribe(e=>this.isConnected=e),this.wsService.telemetry$.subscribe(e=>{e&&(this.telemetry=e,e.network&&(this.netSpeedText=`DN: ${e.network.download_speed_kbps} KB/s | UP: ${e.network.upload_speed_kbps} KB/s`))}),this.wsService.agents$.subscribe(e=>{e&&e.length>0&&(this.agents=e)}),this.apiService.getTelemetry().subscribe(e=>this.telemetry=e),this.apiService.getAgentStatus().subscribe(e=>{e.agents&&(this.agents=e.agents)}),this.audioService.playSciFiTone("boot")}setTab(e){this.audioService.playSciFiTone("beep"),this.activeTab=e}setMobileView(e){this.audioService.playSciFiTone("beep"),this.mobileView=e}onAgentSelected(e){this.selectedAgentId=e,this.chatConsole&&this.chatConsole.addSystemMessage("JARVIS",`Routing requests to [${e.toUpperCase()}], sir.`)}onScreenshotCaptured(e){this.chatConsole&&this.chatConsole.addSystemMessage("JARVIS","Display buffer captured and archived to database, sir.",e)}onAppLaunched(e){this.chatConsole&&this.chatConsole.addSystemMessage("JARVIS",`Initiating ${e.toUpperCase()}, sir.`)}onAgentResponse(e){e.agents&&(this.agents=e.agents)}static{this.\u0275fac=function(t){return new(t||n)(Se(Au),Se(Qt),Se(Ut))}}static{this.\u0275cmp=Lt({type:n,selectors:[["app-root"]],viewQuery:function(t,i){if(t&1&&Zi(Ia,5),t&2){let r;Ji(r=Ki())&&(i.chatConsole=r.first)}},hostBindings:function(t,i){t&1&&Ie("resize",function(){return i.onResize()},!1,jo)},decls:34,vars:31,consts:[[1,"jarvis-container"],[3,"screenshotCaptured","isConnected","netSpeedText"],["class","mobile-nav-bar",4,"ngIf"],[1,"workspace-grid"],[1,"col-panel","left-col"],[3,"appLaunched","telemetry"],[1,"center-column"],[1,"hologram-wrapper"],[1,"chat-wrapper"],[3,"agentResponse","selectedAgent"],[1,"cyber-panel","right-col"],[1,"tabs-nav"],[1,"tab-btn",3,"click"],[1,"fas","fa-network-wired"],[1,"fas","fa-database"],[1,"fas","fa-tasks"],[1,"fas","fa-terminal"],[1,"tab-content"],[3,"agentSelected","agents","selectedAgentId"],[1,"mobile-nav-bar"],[1,"mob-btn",3,"click"],[1,"fas","fa-tachometer-alt"],[1,"fas","fa-comments"],[1,"fas","fa-users-cog"]],template:function(t,i){t&1&&(V(0,"div",0)(1,"app-hud-header",1),Ie("screenshotCaptured",function(s){return i.onScreenshotCaptured(s)}),G(),kt(2,IP,10,6,"div",2),V(3,"main",3)(4,"div",4)(5,"app-system-diagnostics",5),Ie("appLaunched",function(s){return i.onAppLaunched(s)}),G()(),V(6,"section",6)(7,"div",7),ve(8,"app-hologram-viewport"),G(),V(9,"div",8)(10,"app-chat-console",9),Ie("agentResponse",function(s){return i.onAgentResponse(s)}),G()()(),V(11,"section",10)(12,"div",11)(13,"button",12),Ie("click",function(){return i.setTab("agents")}),ve(14,"i",13),ee(15," AGENTS "),G(),V(16,"button",12),Ie("click",function(){return i.setTab("storage")}),ve(17,"i",14),ee(18," 50GB VAULT "),G(),V(19,"button",12),Ie("click",function(){return i.setTab("processes")}),ve(20,"i",15),ee(21," PROCESSES "),G(),V(22,"button",12),Ie("click",function(){return i.setTab("terminal")}),ve(23,"i",16),ee(24," SHELL "),G()(),V(25,"div",17)(26,"app-multi-agent-swarm",18),Ie("agentSelected",function(s){return i.onAgentSelected(s)}),G()(),V(27,"div",17),ve(28,"app-storage-vault"),G(),V(29,"div",17),ve(30,"app-process-manager"),G(),V(31,"div",17),ve(32,"app-terminal-runner"),G()()(),ve(33,"app-hud-footer"),G()),t&2&&(ne(),Ye("isConnected",i.isConnected)("netSpeedText",i.netSpeedText),ne(),Ye("ngIf",i.isMobile),ne(),_t("mobile-mode",i.isMobile),ne(),_t("mob-hidden",i.isMobile&&i.mobileView!=="diagnostics"),ne(),Ye("telemetry",i.telemetry),ne(),_t("mob-hidden",i.isMobile&&i.mobileView!=="chat"),ne(4),Ye("selectedAgent",i.selectedAgentId),ne(),_t("mob-hidden",i.isMobile&&i.mobileView!=="swarm"),ne(2),_t("active",i.activeTab==="agents"),ne(3),_t("active",i.activeTab==="storage"),ne(3),_t("active",i.activeTab==="processes"),ne(3),_t("active",i.activeTab==="terminal"),ne(3),_t("active",i.activeTab==="agents"),ne(),Ye("agents",i.agents)("selectedAgentId",i.selectedAgentId),ne(),_t("active",i.activeTab==="storage"),ne(2),_t("active",i.activeTab==="processes"),ne(2),_t("active",i.activeTab==="terminal"))},dependencies:[Rt,Di,ul,hu,Eu,Ia,Su,wu,Cu,Tu,Du],styles:[".jarvis-container[_ngcontent-%COMP%]{display:grid;grid-template-rows:auto 1fr auto;height:100vh;width:100vw;padding:8px 12px;gap:8px;box-sizing:border-box}.mobile-nav-bar[_ngcontent-%COMP%]{display:flex;gap:6px;background:var(--bg-panel);border:1px solid var(--border-cyan);border-radius:6px;padding:4px}.mob-btn[_ngcontent-%COMP%]{flex:1;padding:8px 4px;background:transparent;border:1px solid transparent;border-radius:4px;color:var(--text-dim);font-family:var(--font-hud);font-size:11px;letter-spacing:1px;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;transition:all .2s}.mob-btn.active[_ngcontent-%COMP%]{background:#00f0ff33;border-color:var(--neon-cyan);color:var(--neon-cyan);box-shadow:0 0 8px #00f0ff4d}.workspace-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:310px 1fr 340px;gap:8px;height:calc(100vh - 116px);min-height:0;overflow:hidden}.workspace-grid.mobile-mode[_ngcontent-%COMP%]{display:flex;flex-direction:column;height:calc(100vh - 160px)}.col-panel[_ngcontent-%COMP%]{height:100%;min-height:0}.center-column[_ngcontent-%COMP%]{display:grid;grid-template-rows:240px 1fr;gap:8px;height:100%;min-height:0}.hologram-wrapper[_ngcontent-%COMP%], .chat-wrapper[_ngcontent-%COMP%]{height:100%;min-height:0}.cyber-panel[_ngcontent-%COMP%]{background:var(--bg-panel);border:1px solid var(--border-cyan);border-radius:8px;display:flex;flex-direction:column;backdrop-filter:blur(14px);box-shadow:0 4px 20px #00000080;position:relative;overflow:hidden;height:100%;min-height:0}.tabs-nav[_ngcontent-%COMP%]{display:flex;background:#00f0ff0d;border-bottom:1px solid var(--border-cyan);flex-shrink:0}.tab-btn[_ngcontent-%COMP%]{flex:1;padding:8px 4px;background:transparent;border:none;color:var(--text-dim);font-family:var(--font-hud);font-size:10px;letter-spacing:1px;cursor:pointer;transition:all .2s}.tab-btn.active[_ngcontent-%COMP%]{color:var(--neon-cyan);background:#00f0ff26;border-bottom:2px solid var(--neon-cyan)}.tab-content[_ngcontent-%COMP%]{flex:1;overflow-y:auto;padding:10px;display:none;min-height:0}.tab-content.active[_ngcontent-%COMP%]{display:block}.mob-hidden[_ngcontent-%COMP%]{display:none!important}@media(max-width:1200px){.workspace-grid[_ngcontent-%COMP%]:not(.mobile-mode){grid-template-columns:280px 1fr 300px}}@media(max-width:992px){.jarvis-container[_ngcontent-%COMP%]{height:100vh}.center-column[_ngcontent-%COMP%]{grid-template-rows:200px 1fr}}"]})}};mh(Iu,{providers:[yh()]}).catch(n=>console.error(n));
