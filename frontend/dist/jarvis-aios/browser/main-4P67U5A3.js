var Kx=Object.defineProperty,Qx=Object.defineProperties;var eb=Object.getOwnPropertyDescriptors;var zm=Object.getOwnPropertySymbols;var tb=Object.prototype.hasOwnProperty,nb=Object.prototype.propertyIsEnumerable;var Gm=(n,e,t)=>e in n?Kx(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,dt=(n,e)=>{for(var t in e||={})tb.call(e,t)&&Gm(n,t,e[t]);if(zm)for(var t of zm(e))nb.call(e,t)&&Gm(n,t,e[t]);return n},Ct=(n,e)=>Qx(n,eb(e));var vs=(n,e,t)=>new Promise((i,r)=>{var s=l=>{try{a(t.next(l))}catch(c){r(c)}},o=l=>{try{a(t.throw(l))}catch(c){r(c)}},a=l=>l.done?i(l.value):Promise.resolve(l.value).then(s,o);a((t=t.apply(n,e)).next())});function Uu(n,e){return Object.is(n,e)}var zt=null,Fa=!1,Vu=1,zn=Symbol("SIGNAL");function Je(n){let e=zt;return zt=n,e}function Bu(){return zt}var Co={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function ka(n){if(Fa)throw new Error("");if(zt===null)return;zt.consumerOnSignalRead(n);let e=zt.nextProducerIndex++;if(Ba(zt),e<zt.producerNode.length&&zt.producerNode[e]!==n&&wo(zt)){let t=zt.producerNode[e];Va(t,zt.producerIndexOfThis[e])}zt.producerNode[e]!==n&&(zt.producerNode[e]=n,zt.producerIndexOfThis[e]=wo(zt)?jm(n,zt,e):0),zt.producerLastReadVersion[e]=n.version}function Wm(){Vu++}function Hu(n){if(!(wo(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Vu)){if(!n.producerMustRecompute(n)&&!ju(n)){ku(n);return}n.producerRecomputeValue(n),ku(n)}}function zu(n){if(n.liveConsumerNode===void 0)return;let e=Fa;Fa=!0;try{for(let t of n.liveConsumerNode)t.dirty||ib(t)}finally{Fa=e}}function Gu(){return zt?.consumerAllowSignalWrites!==!1}function ib(n){n.dirty=!0,zu(n),n.consumerMarkedDirty?.(n)}function ku(n){n.dirty=!1,n.lastCleanEpoch=Vu}function Ua(n){return n&&(n.nextProducerIndex=0),Je(n)}function Wu(n,e){if(Je(e),!(!n||n.producerNode===void 0||n.producerIndexOfThis===void 0||n.producerLastReadVersion===void 0)){if(wo(n))for(let t=n.nextProducerIndex;t<n.producerNode.length;t++)Va(n.producerNode[t],n.producerIndexOfThis[t]);for(;n.producerNode.length>n.nextProducerIndex;)n.producerNode.pop(),n.producerLastReadVersion.pop(),n.producerIndexOfThis.pop()}}function ju(n){Ba(n);for(let e=0;e<n.producerNode.length;e++){let t=n.producerNode[e],i=n.producerLastReadVersion[e];if(i!==t.version||(Hu(t),i!==t.version))return!0}return!1}function $u(n){if(Ba(n),wo(n))for(let e=0;e<n.producerNode.length;e++)Va(n.producerNode[e],n.producerIndexOfThis[e]);n.producerNode.length=n.producerLastReadVersion.length=n.producerIndexOfThis.length=0,n.liveConsumerNode&&(n.liveConsumerNode.length=n.liveConsumerIndexOfThis.length=0)}function jm(n,e,t){if($m(n),n.liveConsumerNode.length===0&&qm(n))for(let i=0;i<n.producerNode.length;i++)n.producerIndexOfThis[i]=jm(n.producerNode[i],n,i);return n.liveConsumerIndexOfThis.push(t),n.liveConsumerNode.push(e)-1}function Va(n,e){if($m(n),n.liveConsumerNode.length===1&&qm(n))for(let i=0;i<n.producerNode.length;i++)Va(n.producerNode[i],n.producerIndexOfThis[i]);let t=n.liveConsumerNode.length-1;if(n.liveConsumerNode[e]=n.liveConsumerNode[t],n.liveConsumerIndexOfThis[e]=n.liveConsumerIndexOfThis[t],n.liveConsumerNode.length--,n.liveConsumerIndexOfThis.length--,e<n.liveConsumerNode.length){let i=n.liveConsumerIndexOfThis[e],r=n.liveConsumerNode[e];Ba(r),r.producerIndexOfThis[i]=e}}function wo(n){return n.consumerIsAlwaysLive||(n?.liveConsumerNode?.length??0)>0}function Ba(n){n.producerNode??=[],n.producerIndexOfThis??=[],n.producerLastReadVersion??=[]}function $m(n){n.liveConsumerNode??=[],n.liveConsumerIndexOfThis??=[]}function qm(n){return n.producerNode!==void 0}function qu(n,e){let t=Object.create(rb);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(Hu(t),ka(t),t.value===La)throw t.error;return t.value};return i[zn]=t,i}var Fu=Symbol("UNSET"),Lu=Symbol("COMPUTING"),La=Symbol("ERRORED"),rb=Ct(dt({},Co),{value:Fu,dirty:!0,error:null,equal:Uu,kind:"computed",producerMustRecompute(n){return n.value===Fu||n.value===Lu},producerRecomputeValue(n){if(n.value===Lu)throw new Error("Detected cycle in computations.");let e=n.value;n.value=Lu;let t=Ua(n),i,r=!1;try{i=n.computation(),Je(null),r=e!==Fu&&e!==La&&i!==La&&n.equal(e,i)}catch(s){i=La,n.error=s}finally{Wu(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function sb(){throw new Error}var Xm=sb;function Ym(n){Xm(n)}function Xu(n){Xm=n}var ob=null;function Yu(n,e){let t=Object.create(Ju);t.value=n,e!==void 0&&(t.equal=e);let i=()=>(ka(t),t.value);return i[zn]=t,i}function Ha(n,e){Gu()||Ym(n),n.equal(n.value,e)||(n.value=e,ab(n))}function Zu(n,e){Gu()||Ym(n),Ha(n,e(n.value))}var Ju=Ct(dt({},Co),{equal:Uu,value:void 0,kind:"signal"});function ab(n){n.version++,Wm(),zu(n),ob?.()}function Ku(n){let e=Je(null);try{return n()}finally{Je(e)}}var Qu;function To(){return Qu}function mi(n){let e=Qu;return Qu=n,e}var za=Symbol("NotFound");function Ke(n){return typeof n=="function"}function Ga(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Wa=Ga(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Do(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var rn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Ke(i))try{i()}catch(s){e=s instanceof Wa?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{Zm(s)}catch(o){e=e??[],o instanceof Wa?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Wa(e)}}add(e){var t;if(e&&e!==this)if(this.closed)Zm(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Do(t,e)}remove(e){let{_finalizers:t}=this;t&&Do(t,e),e instanceof n&&e._removeParent(this)}};rn.EMPTY=(()=>{let n=new rn;return n.closed=!0,n})();var ed=rn.EMPTY;function ja(n){return n instanceof rn||n&&"closed"in n&&Ke(n.remove)&&Ke(n.add)&&Ke(n.unsubscribe)}function Zm(n){Ke(n)?n():n.unsubscribe()}var Gn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ys={setTimeout(n,e,...t){let{delegate:i}=ys;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=ys;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function $a(n){ys.setTimeout(()=>{let{onUnhandledError:e}=Gn;if(e)e(n);else throw n})}function td(){}var Jm=nd("C",void 0,void 0);function Km(n){return nd("E",void 0,n)}function Qm(n){return nd("N",n,void 0)}function nd(n,e,t){return{kind:n,value:e,error:t}}var Mr=null;function _s(n){if(Gn.useDeprecatedSynchronousErrorHandling){let e=!Mr;if(e&&(Mr={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Mr;if(Mr=null,t)throw i}}else n()}function eg(n){Gn.useDeprecatedSynchronousErrorHandling&&Mr&&(Mr.errorThrown=!0,Mr.error=n)}var Er=class extends rn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,ja(e)&&e.add(this)):this.destination=hb}static create(e,t,i){return new xs(e,t,i)}next(e){this.isStopped?rd(Qm(e),this):this._next(e)}error(e){this.isStopped?rd(Km(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?rd(Jm,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},db=Function.prototype.bind;function id(n,e){return db.call(n,e)}var sd=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){qa(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){qa(i)}else qa(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){qa(t)}}},xs=class extends Er{constructor(e,t,i){super();let r;if(Ke(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&Gn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&id(e.next,s),error:e.error&&id(e.error,s),complete:e.complete&&id(e.complete,s)}):r=e}this.destination=new sd(r)}};function qa(n){Gn.useDeprecatedSynchronousErrorHandling?eg(n):$a(n)}function fb(n){throw n}function rd(n,e){let{onStoppedNotification:t}=Gn;t&&ys.setTimeout(()=>t(n,e))}var hb={closed:!0,next:td,error:fb,complete:td};var bs=typeof Symbol=="function"&&Symbol.observable||"@@observable";function tg(n){return n}function ng(n){return n.length===0?tg:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Lt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=mb(t)?t:new xs(t,i,r);return _s(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=ig(i),new i((r,s)=>{let o=new xs({next:a=>{try{t(a)}catch(l){s(l),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[bs](){return this}pipe(...t){return ng(t)(this)}toPromise(t){return t=ig(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function ig(n){var e;return(e=n??Gn.Promise)!==null&&e!==void 0?e:Promise}function pb(n){return n&&Ke(n.next)&&Ke(n.error)&&Ke(n.complete)}function mb(n){return n&&n instanceof Er||pb(n)&&ja(n)}function gb(n){return Ke(n?.lift)}function un(n){return e=>{if(gb(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function dn(n,e,t,i,r){return new od(n,e,t,i,r)}var od=class extends Er{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(l){e.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){e.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var rg=Ga(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var fn=(()=>{class n extends Lt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Xa(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new rg}next(t){_s(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){_s(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){_s(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?ed:(this.currentObservers=null,s.push(t),new rn(()=>{this.currentObservers=null,Do(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Lt;return t.source=this,t}}return n.create=(e,t)=>new Xa(e,t),n})(),Xa=class extends fn{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:ed}};var gi=class extends fn{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function sg(n){return n&&Ke(n.schedule)}function og(n){return n[n.length-1]}function ag(n){return Ke(og(n))?n.pop():void 0}function lg(n){return sg(og(n))?n.pop():void 0}function ug(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{c(i.next(u))}catch(d){o(d)}}function l(u){try{c(i.throw(u))}catch(d){o(d)}}function c(u){u.done?s(u.value):r(u.value).then(a,l)}c((i=i.apply(n,e||[])).next())})}function cg(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Sr(n){return this instanceof Sr?(this.v=n,this):new Sr(n)}function dg(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(f){return function(g){return Promise.resolve(g).then(f,d)}}function a(f,g){i[f]&&(r[f]=function(v){return new Promise(function(m,p){s.push([f,v,m,p])>1||l(f,v)})},g&&(r[f]=g(r[f])))}function l(f,g){try{c(i[f](g))}catch(v){h(s[0][3],v)}}function c(f){f.value instanceof Sr?Promise.resolve(f.value.v).then(u,d):h(s[0][2],f)}function u(f){l("next",f)}function d(f){l("throw",f)}function h(f,g){f(g),s.shift(),s.length&&l(s[0][0],s[0][1])}}function fg(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof cg=="function"?cg(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,l){o=n[s](o),r(a,l,o.done,o.value)})}}function r(s,o,a,l){Promise.resolve(l).then(function(c){s({value:c,done:a})},o)}}var Ya=n=>n&&typeof n.length=="number"&&typeof n!="function";function Za(n){return Ke(n?.then)}function Ja(n){return Ke(n[bs])}function Ka(n){return Symbol.asyncIterator&&Ke(n?.[Symbol.asyncIterator])}function Qa(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function vb(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var el=vb();function tl(n){return Ke(n?.[el])}function nl(n){return dg(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield Sr(t.read());if(r)return yield Sr(void 0);yield yield Sr(i)}}finally{t.releaseLock()}})}function il(n){return Ke(n?.getReader)}function hn(n){if(n instanceof Lt)return n;if(n!=null){if(Ja(n))return yb(n);if(Ya(n))return _b(n);if(Za(n))return xb(n);if(Ka(n))return hg(n);if(tl(n))return bb(n);if(il(n))return Mb(n)}throw Qa(n)}function yb(n){return new Lt(e=>{let t=n[bs]();if(Ke(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function _b(n){return new Lt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function xb(n){return new Lt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,$a)})}function bb(n){return new Lt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function hg(n){return new Lt(e=>{Eb(n,e).catch(t=>e.error(t))})}function Mb(n){return hg(nl(n))}function Eb(n,e){var t,i,r,s;return ug(this,void 0,void 0,function*(){try{for(t=fg(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function An(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function rl(n,e=0){return un((t,i)=>{t.subscribe(dn(i,r=>An(i,n,()=>i.next(r),e),()=>An(i,n,()=>i.complete(),e),r=>An(i,n,()=>i.error(r),e)))})}function sl(n,e=0){return un((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function pg(n,e){return hn(n).pipe(sl(e),rl(e))}function mg(n,e){return hn(n).pipe(sl(e),rl(e))}function gg(n,e){return new Lt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function vg(n,e){return new Lt(t=>{let i;return An(t,e,()=>{i=n[el](),An(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>Ke(i?.return)&&i.return()})}function ol(n,e){if(!n)throw new Error("Iterable cannot be null");return new Lt(t=>{An(t,e,()=>{let i=n[Symbol.asyncIterator]();An(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function yg(n,e){return ol(nl(n),e)}function _g(n,e){if(n!=null){if(Ja(n))return pg(n,e);if(Ya(n))return gg(n,e);if(Za(n))return mg(n,e);if(Ka(n))return ol(n,e);if(tl(n))return vg(n,e);if(il(n))return yg(n,e)}throw Qa(n)}function wr(n,e){return e?_g(n,e):hn(n)}function al(...n){let e=lg(n);return wr(n,e)}function pn(n,e){return un((t,i)=>{let r=0;t.subscribe(dn(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:Sb}=Array;function wb(n,e){return Sb(e)?n(...e):n(e)}function xg(n){return pn(e=>wb(n,e))}var{isArray:Cb}=Array,{getPrototypeOf:Tb,prototype:Db,keys:Ab}=Object;function bg(n){if(n.length===1){let e=n[0];if(Cb(e))return{args:e,keys:null};if(Ib(e)){let t=Ab(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Ib(n){return n&&typeof n=="object"&&Tb(n)===Db}function Mg(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Eg(n,e,t,i,r,s,o,a){let l=[],c=0,u=0,d=!1,h=()=>{d&&!l.length&&!c&&e.complete()},f=v=>c<i?g(v):l.push(v),g=v=>{s&&e.next(v),c++;let m=!1;hn(t(v,u++)).subscribe(dn(e,p=>{r?.(p),s?f(p):e.next(p)},()=>{m=!0},void 0,()=>{if(m)try{for(c--;l.length&&c<i;){let p=l.shift();o?An(e,o,()=>g(p)):g(p)}h()}catch(p){e.error(p)}}))};return n.subscribe(dn(e,f,()=>{d=!0,h()})),()=>{a?.()}}function ll(n,e,t=1/0){return Ke(e)?ll((i,r)=>pn((s,o)=>e(i,s,r,o))(hn(n(i,r))),t):(typeof e=="number"&&(t=e),un((i,r)=>Eg(i,r,n,t)))}function ad(...n){let e=ag(n),{args:t,keys:i}=bg(n),r=new Lt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),l=o,c=o;for(let u=0;u<o;u++){let d=!1;hn(t[u]).subscribe(dn(s,h=>{d||(d=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!d)&&(c||s.next(i?Mg(i,a):a),s.complete())}))}});return e?r.pipe(xg(e)):r}function ld(n,e){return un((t,i)=>{let r=0;t.subscribe(dn(i,s=>n.call(e,s,r++)&&i.next(s)))})}function cd(n,e){return Ke(e)?ll(n,e,1):ll(n,1)}function ud(n){return un((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function dd(n,e){return un((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(dn(i,l=>{r?.unsubscribe();let c=0,u=s++;hn(n(l,u)).subscribe(r=dn(i,d=>i.next(e?e(l,d,u,c++):d),()=>{r=null,a()}))},()=>{o=!0,a()}))})}var uv="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",De=class extends Error{code;constructor(e,t){super(Nb(e,t)),this.code=e}};function Rb(n){return`NG0${Math.abs(n)}`}function Nb(n,e){return`${Rb(n)}${e?": "+e:""}`}function Go(n){return{toString:n}.toString()}var cl="__parameters__";function Pb(n){return function(...t){if(n){let i=n(...t);for(let r in i)this[r]=i[r]}}}function dv(n,e,t){return Go(()=>{let i=Pb(e);function r(...s){if(this instanceof r)return i.apply(this,s),this;let o=new r(...s);return a.annotation=o,a;function a(l,c,u){let d=l.hasOwnProperty(cl)?l[cl]:Object.defineProperty(l,cl,{value:[]})[cl];for(;d.length<=u;)d.push(null);return(d[u]=d[u]||[]).push(o),l}}return r.prototype.ngMetadataName=n,r.annotationCls=r,r})}function Et(n){for(let e in n)if(n[e]===Et)return e;throw Error("Could not find renamed property on target object.")}function Ob(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function Rn(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Rn).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Sg(n,e){return n?e?`${n} ${e}`:n:e||""}var Fb=Et({__forward_ref__:Et});function Ur(n){return n.__forward_ref__=Ur,n.toString=function(){return Rn(this())},n}function sn(n){return fv(n)?n():n}function fv(n){return typeof n=="function"&&n.hasOwnProperty(Fb)&&n.__forward_ref__===Ur}function it(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Vr(n){return{providers:n.providers||[],imports:n.imports||[]}}function yf(n){return wg(n,hv)||wg(n,pv)}function wg(n,e){return n.hasOwnProperty(e)?n[e]:null}function Lb(n){let e=n&&(n[hv]||n[pv]);return e||null}function Cg(n){return n&&(n.hasOwnProperty(Tg)||n.hasOwnProperty(kb))?n[Tg]:null}var hv=Et({\u0275prov:Et}),Tg=Et({\u0275inj:Et}),pv=Et({ngInjectableDef:Et}),kb=Et({ngInjectorDef:Et}),Ne=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=it({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function mv(n){return n&&!!n.\u0275providers}var Ub=Et({\u0275cmp:Et}),Vb=Et({\u0275dir:Et}),Bb=Et({\u0275pipe:Et});var vl=Et({\u0275fac:Et}),No=Et({__NG_ELEMENT_ID__:Et}),Dg=Et({__NG_ENV_ID__:Et});function Po(n){return typeof n=="string"?n:n==null?"":String(n)}function Hb(n){return typeof n=="function"?n.name||n.toString():typeof n=="object"&&n!=null&&typeof n.type=="function"?n.type.name||n.type.toString():Po(n)}function gv(n,e){throw new De(-200,n)}function _f(n,e){throw new De(-201,!1)}var je=(function(n){return n[n.Default=0]="Default",n[n.Host=1]="Host",n[n.Self=2]="Self",n[n.SkipSelf=4]="SkipSelf",n[n.Optional=8]="Optional",n})(je||{}),Cd;function vv(){return Cd}function mn(n){let e=Cd;return Cd=n,e}function yv(n,e,t){let i=yf(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&je.Optional)return null;if(e!==void 0)return e;_f(n,"Injector")}var zb={},Cr=zb,Td="__NG_DI_FLAG__",yl=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=t;return this.injector.get(e,i.optional?za:Cr,i)}},_l="ngTempTokenPath",Gb="ngTokenPath",Wb=/\n/gm,jb="\u0275",Ag="__source";function $b(n,e=je.Default){if(To()===void 0)throw new De(-203,!1);if(To()===null)return yv(n,void 0,e);{let t=To(),i;return t instanceof yl?i=t.injector:i=t,i.get(n,e&je.Optional?null:void 0,e)}}function Ze(n,e=je.Default){return(vv()||$b)(sn(n),e)}function Ye(n,e=je.Default){return Ze(n,Wl(e))}function Wl(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function Dd(n){let e=[];for(let t=0;t<n.length;t++){let i=sn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new De(900,!1);let r,s=je.Default;for(let o=0;o<i.length;o++){let a=i[o],l=qb(a);typeof l=="number"?l===-1?r=a.token:s|=l:r=a}e.push(Ze(r,s))}else e.push(Ze(i))}return e}function _v(n,e){return n[Td]=e,n.prototype[Td]=e,n}function qb(n){return n[Td]}function Xb(n,e,t,i){let r=n[_l];throw e[Ag]&&r.unshift(e[Ag]),n.message=Yb(`
`+n.message,r,t,i),n[Gb]=r,n[_l]=null,n}function Yb(n,e,t,i=null){n=n&&n.charAt(0)===`
`&&n.charAt(1)==jb?n.slice(2):n;let r=Rn(e);if(Array.isArray(e))r=e.map(Rn).join(" -> ");else if(typeof e=="object"){let s=[];for(let o in e)if(e.hasOwnProperty(o)){let a=e[o];s.push(o+":"+(typeof a=="string"?JSON.stringify(a):Rn(a)))}r=`{${s.join(", ")}}`}return`${t}${i?"("+i+")":""}[${r}]: ${n.replace(Wb,`
  `)}`}var Zb=_v(dv("Optional"),8);var Jb=_v(dv("SkipSelf"),4);function Dr(n,e){let t=n.hasOwnProperty(vl);return t?n[vl]:null}function Kb(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function Qb(n){return n.flat(Number.POSITIVE_INFINITY)}function xf(n,e){n.forEach(t=>Array.isArray(t)?xf(t,e):e(t))}function xv(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function xl(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function eM(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function tM(n,e,t){let i=Wo(n,e);return i>=0?n[i|1]=t:(i=~i,eM(n,i,e,t)),i}function fd(n,e){let t=Wo(n,e);if(t>=0)return n[t|1]}function Wo(n,e){return nM(n,e,1)}function nM(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var Ar={},In=[],bl=new Ne(""),bv=new Ne("",-1),Mv=new Ne(""),Ml=class{get(e,t=Cr){if(t===Cr){let i=new Error(`NullInjectorError: No provider for ${Rn(e)}!`);throw i.name="NullInjectorError",i}return t}};function Oo(n){return n[Ub]||null}function iM(n){return n[Vb]||null}function rM(n){return n[Bb]||null}function Ev(n){return{\u0275providers:n}}function sM(...n){return{\u0275providers:Sv(!0,n),\u0275fromNgModule:!0}}function Sv(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return xf(e,o=>{let a=o;Ad(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&wv(r,s),t}function wv(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];bf(r,s=>{e(s,i)})}}function Ad(n,e,t,i){if(n=sn(n),!n)return!1;let r=null,s=Cg(n),o=!s&&Oo(n);if(!s&&!o){let l=n.ngModule;if(s=Cg(l),s)r=l;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let l=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let c of l)Ad(c,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let c;xf(s.imports,u=>{Ad(u,e,t,i)&&(c||=[],c.push(u))}),c!==void 0&&wv(c,e)}if(!a){let c=Dr(r)||(()=>new r);e({provide:r,useFactory:c,deps:In},r),e({provide:Mv,useValue:r,multi:!0},r),e({provide:bl,useValue:()=>Ze(r),multi:!0},r)}let l=s.providers;if(l!=null&&!a){let c=n;bf(l,u=>{e(u,c)})}}else return!1;return r!==n&&n.providers!==void 0}function bf(n,e){for(let t of n)mv(t)&&(t=t.\u0275providers),Array.isArray(t)?bf(t,e):e(t)}var oM=Et({provide:String,useValue:Et});function Cv(n){return n!==null&&typeof n=="object"&&oM in n}function aM(n){return!!(n&&n.useExisting)}function lM(n){return!!(n&&n.useFactory)}function Cs(n){return typeof n=="function"}function cM(n){return!!n.useClass}var jl=new Ne(""),dl={},Ig={},hd;function Mf(){return hd===void 0&&(hd=new Ml),hd}var ii=class{},Fo=class extends ii{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Rd(e,o=>this.processProvider(o)),this.records.set(bv,Ms(void 0,this)),r.has("environment")&&this.records.set(ii,Ms(void 0,this));let s=this.records.get(jl);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(Mv,In,je.Self))}retrieve(e,t){let i=t;return this.get(e,i.optional?za:Cr,i)}destroy(){Io(this),this._destroyed=!0;let e=Je(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Je(e)}}onDestroy(e){return Io(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Io(this);let t=mi(this),i=mn(void 0),r;try{return e()}finally{mi(t),mn(i)}}get(e,t=Cr,i=je.Default){if(Io(this),e.hasOwnProperty(Dg))return e[Dg](this);i=Wl(i);let r,s=mi(this),o=mn(void 0);try{if(!(i&je.SkipSelf)){let l=this.records.get(e);if(l===void 0){let c=pM(e)&&yf(e);c&&this.injectableDefInScope(c)?l=Ms(Id(e),dl):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,i)}let a=i&je.Self?Mf():this.parent;return t=i&je.Optional&&t===Cr?null:t,a.get(e,t)}catch(a){if(a.name==="NullInjectorError"){if((a[_l]=a[_l]||[]).unshift(Rn(e)),s)throw a;return Xb(a,e,"R3InjectorError",this.source)}else throw a}finally{mn(o),mi(s)}}resolveInjectorInitializers(){let e=Je(null),t=mi(this),i=mn(void 0),r;try{let s=this.get(bl,In,je.Self);for(let o of s)o()}finally{mi(t),mn(i),Je(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(Rn(i));return`R3Injector[${e.join(", ")}]`}processProvider(e){e=sn(e);let t=Cs(e)?e:sn(e&&e.provide),i=dM(e);if(!Cs(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Ms(void 0,dl,!0),r.factory=()=>Dd(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Je(null);try{return t.value===Ig?gv(Rn(e)):t.value===dl&&(t.value=Ig,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&hM(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Je(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=sn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Id(n){let e=yf(n),t=e!==null?e.factory:Dr(n);if(t!==null)return t;if(n instanceof Ne)throw new De(204,!1);if(n instanceof Function)return uM(n);throw new De(204,!1)}function uM(n){if(n.length>0)throw new De(204,!1);let t=Lb(n);return t!==null?()=>t.factory(n):()=>new n}function dM(n){if(Cv(n))return Ms(void 0,n.useValue);{let e=Tv(n);return Ms(e,dl)}}function Tv(n,e,t){let i;if(Cs(n)){let r=sn(n);return Dr(r)||Id(r)}else if(Cv(n))i=()=>sn(n.useValue);else if(lM(n))i=()=>n.useFactory(...Dd(n.deps||[]));else if(aM(n))i=(r,s)=>Ze(sn(n.useExisting),s!==void 0&&s&je.Optional?je.Optional:void 0);else{let r=sn(n&&(n.useClass||n.provide));if(fM(n))i=()=>new r(...Dd(n.deps));else return Dr(r)||Id(r)}return i}function Io(n){if(n.destroyed)throw new De(205,!1)}function Ms(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function fM(n){return!!n.deps}function hM(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function pM(n){return typeof n=="function"||typeof n=="object"&&n instanceof Ne}function Rd(n,e){for(let t of n)Array.isArray(t)?Rd(t,e):t&&mv(t)?Rd(t.\u0275providers,e):e(t)}function $l(n,e){let t;n instanceof Fo?(Io(n),t=n):t=new yl(n);let i,r=mi(t),s=mn(void 0);try{return e()}finally{mi(r),mn(s)}}function mM(){return vv()!==void 0||To()!=null}function gM(n){return typeof n=="function"}var bi=0,We=1,Le=2,Kt=3,jn=4,$n=5,El=6,Sl=7,gn=8,Ts=9,vi=10,jt=11,Lo=12,Rg=13,Ns=14,ri=15,Ir=16,Es=17,yi=18,ql=19,Dv=20,Xi=21,pd=22,wl=23,Nn=24,md=25,Pn=26,Av=1;var Rr=7,Cl=8,Ds=9,En=10;function Yi(n){return Array.isArray(n)&&typeof n[Av]=="object"}function Mi(n){return Array.isArray(n)&&n[Av]===!0}function Iv(n){return(n.flags&4)!==0}function Ps(n){return n.componentOffset>-1}function Ef(n){return(n.flags&1)===1}function si(n){return!!n.template}function Tl(n){return(n[Le]&512)!==0}function Os(n){return(n[Le]&256)===256}var Nd=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function Rv(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var Sf=(()=>{let n=()=>Nv;return n.ngInherit=!0,n})();function Nv(n){return n.type.prototype.ngOnChanges&&(n.setInput=yM),vM}function vM(){let n=Ov(this),e=n?.current;if(e){let t=n.previous;if(t===Ar)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function yM(n,e,t,i,r){let s=this.declaredInputs[i],o=Ov(n)||_M(n,{previous:Ar,current:null}),a=o.current||(o.current={}),l=o.previous,c=l[s];a[s]=new Nd(c&&c.currentValue,t,l===Ar),Rv(n,e,r,t)}var Pv="__ngSimpleChanges__";function Ov(n){return n[Pv]||null}function _M(n,e){return n[Pv]=e}var Ng=null;var _t=function(n,e=null,t){Ng?.(n,e,t)},xM="svg",bM="math";function oi(n){for(;Array.isArray(n);)n=n[bi];return n}function Fv(n,e){return oi(e[n])}function Ei(n,e){return oi(e[n.index])}function Lv(n,e){return n.data[e]}function kv(n,e){return n[e]}function MM(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function ai(n,e){let t=e[n];return Yi(t)?t:t[bi]}function EM(n){return(n[Le]&4)===4}function wf(n){return(n[Le]&128)===128}function SM(n){return Mi(n[Kt])}function Dl(n,e){return e==null?null:n[e]}function Uv(n){n[Es]=0}function Vv(n){n[Le]&1024||(n[Le]|=1024,wf(n)&&Yl(n))}function wM(n,e){for(;n>0;)e=e[Ns],n--;return e}function Xl(n){return!!(n[Le]&9216||n[Nn]?.dirty)}function Pd(n){n[vi].changeDetectionScheduler?.notify(8),n[Le]&64&&(n[Le]|=1024),Xl(n)&&Yl(n)}function Yl(n){n[vi].changeDetectionScheduler?.notify(0);let e=Nr(n);for(;e!==null&&!(e[Le]&8192||(e[Le]|=8192,!wf(e)));)e=Nr(e)}function Bv(n,e){if(Os(n))throw new De(911,!1);n[Xi]===null&&(n[Xi]=[]),n[Xi].push(e)}function CM(n,e){if(n[Xi]===null)return;let t=n[Xi].indexOf(e);t!==-1&&n[Xi].splice(t,1)}function Nr(n){let e=n[Kt];return Mi(e)?e[Kt]:e}function Cf(n){return n[Sl]??=[]}function Tf(n){return n.cleanup??=[]}function TM(n,e,t,i){let r=Cf(e);r.push(t),n.firstCreatePass&&Tf(n).push(i,r.length-1)}var ze={lFrame:Yv(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Od=!1;function DM(){return ze.lFrame.elementDepthCount}function AM(){ze.lFrame.elementDepthCount++}function IM(){ze.lFrame.elementDepthCount--}function Hv(){return ze.bindingsEnabled}function RM(){return ze.skipHydrationRootTNode!==null}function NM(n){return ze.skipHydrationRootTNode===n}function PM(){ze.skipHydrationRootTNode=null}function ft(){return ze.lFrame.lView}function on(){return ze.lFrame.tView}function Nt(n){return ze.lFrame.contextLView=n,n[gn]}function Pt(n){return ze.lFrame.contextLView=null,n}function On(){let n=zv();for(;n!==null&&n.type===64;)n=n.parent;return n}function zv(){return ze.lFrame.currentTNode}function OM(){let n=ze.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function jo(n,e){let t=ze.lFrame;t.currentTNode=n,t.isParent=e}function Gv(){return ze.lFrame.isParent}function FM(){ze.lFrame.isParent=!1}function LM(){return ze.lFrame.contextLView}function Wv(){return Od}function Pg(n){let e=Od;return Od=n,e}function kM(){let n=ze.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function UM(){return ze.lFrame.bindingIndex}function VM(n){return ze.lFrame.bindingIndex=n}function Df(){return ze.lFrame.bindingIndex++}function jv(n){let e=ze.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function BM(){return ze.lFrame.inI18n}function HM(n,e){let t=ze.lFrame;t.bindingIndex=t.bindingRootIndex=n,Fd(e)}function zM(){return ze.lFrame.currentDirectiveIndex}function Fd(n){ze.lFrame.currentDirectiveIndex=n}function GM(n){let e=ze.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function $v(){return ze.lFrame.currentQueryIndex}function Af(n){ze.lFrame.currentQueryIndex=n}function WM(n){let e=n[We];return e.type===2?e.declTNode:e.type===1?n[$n]:null}function qv(n,e,t){if(t&je.SkipSelf){let r=e,s=n;for(;r=r.parent,r===null&&!(t&je.Host);)if(r=WM(s),r===null||(s=s[Ns],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=ze.lFrame=Xv();return i.currentTNode=e,i.lView=n,!0}function If(n){let e=Xv(),t=n[We];ze.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function Xv(){let n=ze.lFrame,e=n===null?null:n.child;return e===null?Yv(n):e}function Yv(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function Zv(){let n=ze.lFrame;return ze.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Jv=Zv;function Rf(){let n=Zv();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function jM(n){return(ze.lFrame.contextLView=wM(n,ze.lFrame.contextLView))[gn]}function Br(){return ze.lFrame.selectedIndex}function Pr(n){ze.lFrame.selectedIndex=n}function Kv(){let n=ze.lFrame;return Lv(n.tView,n.selectedIndex)}function Qv(){return ze.lFrame.currentNamespace}var ey=!0;function Nf(){return ey}function Pf(n){ey=n}function $M(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Nv(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function ty(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),l&&(n.viewHooks??=[]).push(-t,l),c&&((n.viewHooks??=[]).push(t,c),(n.viewCheckHooks??=[]).push(t,c)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function fl(n,e,t){ny(n,e,3,t)}function hl(n,e,t,i){(n[Le]&3)===t&&ny(n,e,t,i)}function gd(n,e){let t=n[Le];(t&3)===e&&(t&=16383,t+=1,n[Le]=t)}function ny(n,e,t,i){let r=i!==void 0?n[Es]&65535:0,s=i??-1,o=e.length-1,a=0;for(let l=r;l<o;l++)if(typeof e[l+1]=="number"){if(a=e[l],i!=null&&a>=i)break}else e[l]<0&&(n[Es]+=65536),(a<s||s==-1)&&(qM(n,t,e,l),n[Es]=(n[Es]&4294901760)+l+2),l++}function Og(n,e){_t(4,n,e);let t=Je(null);try{e.call(n)}finally{Je(t),_t(5,n,e)}}function qM(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Le]>>14<n[Es]>>16&&(n[Le]&3)===e&&(n[Le]+=16384,Og(a,s)):Og(a,s)}var ws=-1,Or=class{factory;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i){this.factory=e,this.canSeeViewProviders=t,this.injectImpl=i}};function XM(n){return(n.flags&8)!==0}function YM(n){return(n.flags&16)!==0}function ZM(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];KM(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function JM(n){return n===3||n===4||n===6}function KM(n){return n.charCodeAt(0)===64}function ko(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?Fg(n,t,r,null,e[++i]):Fg(n,t,r,null,null))}}return n}function Fg(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function iy(n){return n!==ws}function Al(n){return n&32767}function QM(n){return n>>16}function Il(n,e){let t=QM(n),i=e;for(;t>0;)i=i[Ns],t--;return i}var Ld=!0;function Rl(n){let e=Ld;return Ld=n,e}var eE=256,ry=eE-1,sy=5,tE=0,ni={};function nE(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(No)&&(i=t[No]),i==null&&(i=t[No]=tE++);let r=i&ry,s=1<<r;e.data[n+(r>>sy)]|=s}function Nl(n,e){let t=oy(n,e);if(t!==-1)return t;let i=e[We];i.firstCreatePass&&(n.injectorIndex=e.length,vd(i.data,n),vd(e,null),vd(i.blueprint,null));let r=Of(n,e),s=n.injectorIndex;if(iy(r)){let o=Al(r),a=Il(r,e),l=a[We].data;for(let c=0;c<8;c++)e[s+c]=a[o+c]|l[o+c]}return e[s+8]=r,s}function vd(n,e){n.push(0,0,0,0,0,0,0,0,e)}function oy(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Of(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=dy(r),i===null)return ws;if(t++,r=r[Ns],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return ws}function kd(n,e,t){nE(n,e,t)}function ay(n,e,t){if(t&je.Optional||n!==void 0)return n;_f(e,"NodeInjector")}function ly(n,e,t,i){if(t&je.Optional&&i===void 0&&(i=null),(t&(je.Self|je.Host))===0){let r=n[Ts],s=mn(void 0);try{return r?r.get(e,i,t&je.Optional):yv(e,i,t&je.Optional)}finally{mn(s)}}return ay(i,e,t)}function cy(n,e,t,i=je.Default,r){if(n!==null){if(e[Le]&2048&&!(i&je.Self)){let o=oE(n,e,t,i,ni);if(o!==ni)return o}let s=uy(n,e,t,i,ni);if(s!==ni)return s}return ly(e,t,i,r)}function uy(n,e,t,i,r){let s=rE(t);if(typeof s=="function"){if(!qv(e,n,i))return i&je.Host?ay(r,t,i):ly(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&je.Optional))_f(t);else return o}finally{Jv()}}else if(typeof s=="number"){let o=null,a=oy(n,e),l=ws,c=i&je.Host?e[ri][$n]:null;for((a===-1||i&je.SkipSelf)&&(l=a===-1?Of(n,e):e[a+8],l===ws||!kg(i,!1)?a=-1:(o=e[We],a=Al(l),e=Il(l,e)));a!==-1;){let u=e[We];if(Lg(s,a,u.data)){let d=iE(a,e,t,o,i,c);if(d!==ni)return d}l=e[a+8],l!==ws&&kg(i,e[We].data[a+8]===c)&&Lg(s,a,e)?(o=u,a=Al(l),e=Il(l,e)):a=-1}}return r}function iE(n,e,t,i,r,s){let o=e[We],a=o.data[n+8],l=i==null?Ps(a)&&Ld:i!=o&&(a.type&3)!==0,c=r&je.Host&&s===a,u=pl(a,o,t,l,c);return u!==null?Uo(e,o,u,a,r):ni}function pl(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,l=n.directiveStart,c=n.directiveEnd,u=s>>20,d=i?a:a+u,h=r?a+u:c;for(let f=d;f<h;f++){let g=o[f];if(f<l&&t===g||f>=l&&g.type===t)return f}if(r){let f=o[l];if(f&&si(f)&&f.type===t)return l}return null}function Uo(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof Or){let a=s;a.resolving&&gv(Hb(o[t]));let l=Rl(a.canSeeViewProviders);a.resolving=!0;let c,u=a.injectImpl?mn(a.injectImpl):null,d=qv(n,i,je.Default);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&$M(t,o[t],e)}finally{u!==null&&mn(u),Rl(l),a.resolving=!1,Jv()}}return s}function rE(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(No)?n[No]:void 0;return typeof e=="number"?e>=0?e&ry:sE:e}function Lg(n,e,t){let i=1<<n;return!!(t[e+(n>>sy)]&i)}function kg(n,e){return!(n&je.Self)&&!(n&je.Host&&e)}var Tr=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return cy(this._tNode,this._lView,e,Wl(i),t)}};function sE(){return new Tr(On(),ft())}function Ff(n){return Go(()=>{let e=n.prototype.constructor,t=e[vl]||Ud(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[vl]||Ud(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function Ud(n){return fv(n)?()=>{let e=Ud(sn(n));return e&&e()}:Dr(n)}function oE(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Le]&2048&&!Tl(o);){let a=uy(s,o,t,i|je.Self,ni);if(a!==ni)return a;let l=s.parent;if(!l){let c=o[Dv];if(c){let u=c.get(t,ni,i);if(u!==ni)return u}l=dy(o),o=o[Ns]}s=l}return r}function dy(n){let e=n[We],t=e.type;return t===2?e.declTNode:t===1?n[$n]:null}function Ug(n,e=null,t=null,i){let r=aE(n,e,t,i);return r.resolveInjectorInitializers(),r}function aE(n,e=null,t=null,i,r=new Set){let s=[t||In,sM(n)];return i=i||(typeof n=="object"?void 0:Rn(n)),new Fo(s,e||Mf(),i||null,r)}var As=class n{static THROW_IF_NOT_FOUND=Cr;static NULL=new Ml;static create(e,t){if(Array.isArray(e))return Ug({name:""},t,e,"");{let i=e.name??"";return Ug({name:i},e.parent,e.providers,i)}}static \u0275prov=it({token:n,providedIn:"any",factory:()=>Ze(bv)});static __NG_ELEMENT_ID__=-1};var lE=new Ne("");lE.__NG_ELEMENT_ID__=n=>{let e=On();if(e===null)throw new De(204,!1);if(e.type&2)return e.value;if(n&je.Optional)return null;throw new De(204,!1)};var fy=!1,Lf=(()=>{class n{static __NG_ELEMENT_ID__=cE;static __NG_ENV_ID__=t=>t}return n})(),Vd=class extends Lf{_lView;constructor(e){super(),this._lView=e}onDestroy(e){let t=this._lView;return Os(t)?(e(),()=>{}):(Bv(t,e),()=>CM(t,e))}};function cE(){return new Vd(ft())}var Vo=class{},hy=new Ne("",{providedIn:"root",factory:()=>!1});var py=new Ne(""),my=new Ne(""),Fs=(()=>{class n{taskId=0;pendingTasks=new Set;get _hasPendingTasks(){return this.hasPendingTasks.value}hasPendingTasks=new gi(!1);add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static \u0275prov=it({token:n,providedIn:"root",factory:()=>new n})}return n})();var Bd=class extends fn{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,mM()&&(this.destroyRef=Ye(Lf,{optional:!0})??void 0,this.pendingTasks=Ye(Fs,{optional:!0})??void 0)}emit(e){let t=Je(null);try{super.next(e)}finally{Je(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let l=e;r=l.next?.bind(l),s=l.error?.bind(l),o=l.complete?.bind(l)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof rn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Rt=Bd;function Pl(...n){}function gy(n){let e,t;function i(){n=Pl;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Vg(n){return queueMicrotask(()=>n()),()=>{n=Pl}}var kf="isAngularZone",Ol=kf+"_ID",uE=0,Jt=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Rt(!1);onMicrotaskEmpty=new Rt(!1);onStable=new Rt(!1);onError=new Rt(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=fy}=e;if(typeof Zone>"u")throw new De(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,hE(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(kf)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new De(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new De(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,dE,Pl,Pl);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},dE={};function Uf(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function fE(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){gy(()=>{n.callbackScheduled=!1,Hd(n),n.isCheckStableRunning=!0,Uf(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Hd(n)}function hE(n){let e=()=>{fE(n)},t=uE++;n._inner=n._inner.fork({name:"angular",properties:{[kf]:!0,[Ol]:t,[Ol+t]:!0},onInvokeTask:(i,r,s,o,a,l)=>{if(pE(l))return i.invokeTask(s,o,a,l);try{return Bg(n),i.invokeTask(s,o,a,l)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Hg(n)}},onInvoke:(i,r,s,o,a,l,c)=>{try{return Bg(n),i.invoke(s,o,a,l,c)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!mE(l)&&e(),Hg(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Hd(n),Uf(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Hd(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Bg(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Hg(n){n._nesting--,Uf(n)}var zd=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Rt;onMicrotaskEmpty=new Rt;onStable=new Rt;onError=new Rt;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function pE(n){return vy(n,"__ignore_ng_zone__")}function mE(n){return vy(n,"__scheduler_tick__")}function vy(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var _i=class{_console=console;handleError(e){this._console.error("ERROR",e)}},gE=new Ne("",{providedIn:"root",factory:()=>{let n=Ye(Jt),e=Ye(_i);return t=>n.runOutsideAngular(()=>e.handleError(t))}});function vE(){return Ls(On(),ft())}function Ls(n,e){return new Si(Ei(n,e))}var Si=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=vE}return n})();function yE(n){return n instanceof Si?n.nativeElement:n}function _E(n){return typeof n=="function"&&n[zn]!==void 0}function $o(n,e){let t=Yu(n,e?.equal),i=t[zn];return t.set=r=>Ha(i,r),t.update=r=>Zu(i,r),t.asReadonly=xE.bind(t),t}function xE(){let n=this[zn];if(n.readonlyFn===void 0){let e=()=>this();e[zn]=n,n.readonlyFn=e}return n.readonlyFn}function yy(n){return _E(n)&&typeof n.set=="function"}function bE(){return this._results[Symbol.iterator]()}var Gd=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new fn}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=Qb(e);(this._changesDetected=!Kb(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=bE};function _y(n){return(n.flags&128)===128}var xy=(function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n})(xy||{}),by=new Map,ME=0;function EE(){return ME++}function SE(n){by.set(n[ql],n)}function Wd(n){by.delete(n[ql])}var zg="__ngContext__";function qo(n,e){Yi(e)?(n[zg]=e[ql],SE(e)):n[zg]=e}function My(n){return Sy(n[Lo])}function Ey(n){return Sy(n[jn])}function Sy(n){for(;n!==null&&!Mi(n);)n=n[jn];return n}var jd;function wy(n){jd=n}function wE(){if(jd!==void 0)return jd;if(typeof document<"u")return document;throw new De(210,!1)}var Vf=new Ne("",{providedIn:"root",factory:()=>CE}),CE="ng",Bf=new Ne(""),Xo=new Ne("",{providedIn:"platform",factory:()=>"unknown"});var Hf=new Ne("",{providedIn:"root",factory:()=>wE().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var TE="h",DE="b";var Cy=!1,AE=new Ne("",{providedIn:"root",factory:()=>Cy});var Ty=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Ty||{}),Zl=new Ne(""),Gg=new Set;function IE(n){Gg.has(n)||(Gg.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var RE=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=it({token:n,providedIn:"root",factory:()=>new n})}return n})();var NE=(n,e,t,i)=>{};function PE(n,e,t,i){NE(n,e,t,i)}var OE=()=>null;function Dy(n,e,t=!1){return OE(n,e,t)}function Ay(n,e){let t=n.contentQueries;if(t!==null){let i=Je(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Af(s),a.contentQueries(2,e[o],o)}}}finally{Je(i)}}}function $d(n,e,t){Af(0);let i=Je(null);try{e(n,t)}finally{Je(i)}}function Iy(n,e,t){if(Iv(e)){let i=Je(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let l=t[o];a.contentQueries(1,l,o)}}}finally{Je(i)}}}var li=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n})(li||{});var Fl=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${uv})`}};function Ry(n){return n instanceof Fl?n.changingThisBreaksApplicationSecurity:n}function FE(n,e){let t=LE(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${uv})`)}return t===e}function LE(n){return n instanceof Fl&&n.getTypeName()||null}var kE=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function UE(n){return n=String(n),n.match(kE)?n:"unsafe:"+n}function VE(n,e){return n.createText(e)}function BE(n,e,t){n.setValue(e,t)}function Ny(n,e,t){return n.createElement(e,t)}function Ll(n,e,t,i,r){n.insertBefore(e,t,i,r)}function Py(n,e,t){n.appendChild(e,t)}function Wg(n,e,t,i,r){i!==null?Ll(n,e,t,i,r):Py(n,e,t)}function HE(n,e,t){n.removeChild(null,e,t)}function zE(n,e,t){n.setAttribute(e,"style",t)}function GE(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Oy(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&ZM(n,e,i),r!==null&&GE(n,e,r),s!==null&&zE(n,e,s)}var Fy=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n[n.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",n})(Fy||{});function Ly(n){let e=WE();return e?e.sanitize(Fy.URL,n)||"":FE(n,"URL")?Ry(n):UE(Po(n))}function WE(){let n=ft();return n&&n[vi].sanitizer}function Yo(n){return n.ownerDocument.defaultView}function jE(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var ky="ng-template";function $E(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&jE(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(zf(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function zf(n){return n.type===4&&n.value!==ky}function qE(n,e,t){let i=n.type===4&&!t?ky:n.value;return e===i}function XE(n,e,t){let i=4,r=n.attrs,s=r!==null?JE(r):0,o=!1;for(let a=0;a<e.length;a++){let l=e[a];if(typeof l=="number"){if(!o&&!Wn(i)&&!Wn(l))return!1;if(o&&Wn(l))continue;o=!1,i=l|i&1;continue}if(!o)if(i&4){if(i=2|i&1,l!==""&&!qE(n,l,t)||l===""&&e.length===1){if(Wn(i))return!1;o=!0}}else if(i&8){if(r===null||!$E(n,r,l,t)){if(Wn(i))return!1;o=!0}}else{let c=e[++a],u=YE(l,r,zf(n),t);if(u===-1){if(Wn(i))return!1;o=!0;continue}if(c!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&c!==d){if(Wn(i))return!1;o=!0}}}}return Wn(i)||o}function Wn(n){return(n&1)===0}function YE(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return KE(e,n)}function ZE(n,e,t=!1){for(let i=0;i<e.length;i++)if(XE(n,e[i],t))return!0;return!1}function JE(n){for(let e=0;e<n.length;e++){let t=n[e];if(JM(t))return e}return n.length}function KE(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function jg(n,e){return n?":not("+e.trim()+")":e}function QE(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!Wn(o)&&(e+=jg(s,r),r=""),i=o,s=s||!Wn(i);t++}return r!==""&&(e+=jg(s,r)),e}function eS(n){return n.map(QE).join(",")}function tS(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!Wn(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var wi={};function Gf(n,e,t,i,r,s,o,a,l,c,u){let d=Pn+i,h=d+r,f=nS(d,h),g=typeof c=="function"?c():c;return f[We]={type:n,blueprint:f,template:t,queries:null,viewQuery:a,declTNode:e,data:f.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:l,consts:g,incompleteFirstPass:!1,ssrId:u}}function nS(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:wi);return t}function iS(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Gf(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Wf(n,e,t,i,r,s,o,a,l,c,u){let d=e.blueprint.slice();return d[bi]=r,d[Le]=i|4|128|8|64|1024,(c!==null||n&&n[Le]&2048)&&(d[Le]|=2048),Uv(d),d[Kt]=d[Ns]=n,d[gn]=t,d[vi]=o||n&&n[vi],d[jt]=a||n&&n[jt],d[Ts]=l||n&&n[Ts]||null,d[$n]=s,d[ql]=EE(),d[El]=u,d[Dv]=c,d[ri]=e.type==2?n[ri]:d,d}function rS(n,e,t){let i=Ei(e,n),r=iS(t),s=n[vi].rendererFactory,o=jf(n,Wf(n,r,null,Uy(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function Uy(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function Vy(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function jf(n,e){return n[Lo]?n[Rg][jn]=e:n[Lo]=e,n[Rg]=e,e}function Q(n=1){By(on(),ft(),Br()+n,!1)}function By(n,e,t,i){if(!i)if((e[Le]&3)===3){let s=n.preOrderCheckHooks;s!==null&&fl(e,s,t)}else{let s=n.preOrderHooks;s!==null&&hl(e,s,0,t)}Pr(t)}var Jl=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(Jl||{});function qd(n,e,t,i){let r=Je(null);try{let[s,o,a]=n.inputs[t],l=null;(o&Jl.SignalBased)!==0&&(l=e[s][zn]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,l,i,t,s):Rv(e,l,s,i)}finally{Je(r)}}function Hy(n,e,t,i,r){let s=Br(),o=i&2;try{Pr(-1),o&&e.length>Pn&&By(n,e,Pn,!1),_t(o?2:0,r),t(i,r)}finally{Pr(s),_t(o?3:1,r)}}function $f(n,e,t){uS(n,e,t),(t.flags&64)===64&&dS(n,e,t)}function zy(n,e,t=Ei){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function sS(n,e,t,i){let s=i.get(AE,Cy)||t===li.ShadowDom,o=n.selectRootElement(e,s);if(o?.tagName?.toLowerCase()==="script")throw new De(905,!1);return oS(o),o}function oS(n){aS(n)}var aS=()=>null;function lS(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function Gy(n,e,t,i,r,s,o,a){if(!a&&qf(e,n,t,i,r)){Ps(e)&&cS(t,e.index);return}if(e.type&3){let l=Ei(e,t);i=lS(i),r=o!=null?o(r,e.value||"",i):r,s.setProperty(l,i,r)}else e.type&12}function cS(n,e){let t=ai(e,n);t[Le]&16||(t[Le]|=64)}function uS(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Ps(t)&&rS(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Nl(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],l=Uo(e,n,o,t);if(qo(l,e),s!==null&&hS(e,o-i,l,a,t,s),si(a)){let c=ai(t.index,e);c[gn]=Uo(e,n,o,t)}}}function dS(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=zM();try{Pr(s);for(let a=i;a<r;a++){let l=n.data[a],c=e[a];Fd(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&fS(l,c)}}finally{Pr(-1),Fd(o)}}function fS(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function Wy(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];ZE(e,s.selectors,!1)&&(i??=[],si(s)?i.unshift(s):i.push(s))}return i}function hS(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let l=o[a],c=o[a+1];qd(i,t,l,c)}}function pS(n,e){let t=n[Ts],i=t?t.get(_i,null):null;i&&i.handleError(e)}function qf(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let l=0;l<o.length;l+=2){let c=o[l],u=o[l+1],d=e.data[c];qd(d,t[c],u,r),a=!0}if(s)for(let l of s){let c=t[l],u=e.data[l];qd(u,c,i,r),a=!0}return a}function mS(n,e){let t=ai(e,n),i=t[We];gS(i,t);let r=t[bi];r!==null&&t[El]===null&&(t[El]=Dy(r,t[Ts])),_t(18),Xf(i,t,t[gn]),_t(19,t[gn])}function gS(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Xf(n,e,t){If(e);try{let i=n.viewQuery;i!==null&&$d(1,i,t);let r=n.template;r!==null&&Hy(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[yi]?.finishViewCreation(n),n.staticContentQueries&&Ay(n,e),n.staticViewQueries&&$d(2,n.viewQuery,t);let s=n.components;s!==null&&vS(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Le]&=-5,Rf()}}function vS(n,e){for(let t=0;t<e.length;t++)mS(n,e[t])}function yS(n,e,t,i){let r=Je(null);try{let s=e.tView,a=n[Le]&4096?4096:16,l=Wf(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=n[e.index];l[Ir]=c;let u=n[yi];return u!==null&&(l[yi]=u.createEmbeddedView(s)),Xf(s,l,t),l}finally{Je(r)}}function $g(n,e){return!e||e.firstChild===null||_y(n)}var _S;function Yf(n,e){return _S(n,e)}var xi=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(xi||{});function jy(n){return(n.flags&32)===32}function Ss(n,e,t,i,r){if(i!=null){let s,o=!1;Mi(i)?s=i:Yi(i)&&(o=!0,i=i[bi]);let a=oi(i);n===0&&t!==null?r==null?Py(e,t,a):Ll(e,t,a,r||null,!0):n===1&&t!==null?Ll(e,t,a,r||null,!0):n===2?HE(e,a,o):n===3&&e.destroyNode(a),s!=null&&RS(e,n,s,t,r)}}function xS(n,e){$y(n,e),e[bi]=null,e[$n]=null}function bS(n,e,t,i,r,s){i[bi]=r,i[$n]=e,Kl(n,i,t,1,r,s)}function $y(n,e){e[vi].changeDetectionScheduler?.notify(9),Kl(n,e,e[jt],2,null,null)}function MS(n){let e=n[Lo];if(!e)return yd(n[We],n);for(;e;){let t=null;if(Yi(e))t=e[Lo];else{let i=e[En];i&&(t=i)}if(!t){for(;e&&!e[jn]&&e!==n;)Yi(e)&&yd(e[We],e),e=e[Kt];e===null&&(e=n),Yi(e)&&yd(e[We],e),t=e&&e[jn]}e=t}}function Zf(n,e){let t=n[Ds],i=t.indexOf(e);t.splice(i,1)}function qy(n,e){if(Os(e))return;let t=e[jt];t.destroyNode&&Kl(n,e,t,3,null,null),MS(e)}function yd(n,e){if(Os(e))return;let t=Je(null);try{e[Le]&=-129,e[Le]|=256,e[Nn]&&$u(e[Nn]),SS(n,e),ES(n,e),e[We].type===1&&e[jt].destroy();let i=e[Ir];if(i!==null&&Mi(e[Kt])){i!==e[Kt]&&Zf(i,e);let r=e[yi];r!==null&&r.detachView(n)}Wd(e)}finally{Je(t)}}function ES(n,e){let t=n.cleanup,i=e[Sl];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[Sl]=null);let r=e[Xi];if(r!==null){e[Xi]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[wl];if(s!==null){e[wl]=null;for(let o of s)o.destroy()}}function SS(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Or)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],l=s[o+1];_t(4,a,l);try{l.call(a)}finally{_t(5,a,l)}}else{_t(4,r,s);try{s.call(r)}finally{_t(5,r,s)}}}}}function wS(n,e,t){return CS(n,e.parent,t)}function CS(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[bi];if(Ps(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===li.None||r===li.Emulated)return null}return Ei(i,t)}function TS(n,e,t){return AS(n,e,t)}function DS(n,e,t){return n.type&40?Ei(n,t):null}var AS=DS,qg;function Jf(n,e,t,i){let r=wS(n,i,e),s=e[jt],o=i.parent||e[$n],a=TS(o,i,e);if(r!=null)if(Array.isArray(t))for(let l=0;l<t.length;l++)Wg(s,r,t[l],a,!1);else Wg(s,r,t,a,!1);qg!==void 0&&qg(s,i,e,t,r)}function Ro(n,e){if(e!==null){let t=e.type;if(t&3)return Ei(e,n);if(t&4)return Xd(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return Ro(n,i);{let r=n[e.index];return Mi(r)?Xd(-1,r):oi(r)}}else{if(t&128)return Ro(n,e.next);if(t&32)return Yf(e,n)()||oi(n[e.index]);{let i=Xy(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Nr(n[ri]);return Ro(r,i)}else return Ro(n,e.next)}}}return null}function Xy(n,e){if(e!==null){let i=n[ri][$n],r=e.projection;return i.projection[r]}return null}function Xd(n,e){let t=En+n+1;if(t<e.length){let i=e[t],r=i[We].firstChild;if(r!==null)return Ro(i,r)}return e[Rr]}function Kf(n,e,t,i,r,s,o){for(;t!=null;){if(t.type===128){t=t.next;continue}let a=i[t.index],l=t.type;if(o&&e===0&&(a&&qo(oi(a),i),t.flags|=2),!jy(t))if(l&8)Kf(n,e,t.child,i,r,s,!1),Ss(e,n,r,a,s);else if(l&32){let c=Yf(t,i),u;for(;u=c();)Ss(e,n,r,u,s);Ss(e,n,r,a,s)}else l&16?IS(n,e,i,t,r,s):Ss(e,n,r,a,s);t=o?t.projectionNext:t.next}}function Kl(n,e,t,i,r,s){Kf(t,i,n.firstChild,e,r,s,!1)}function IS(n,e,t,i,r,s){let o=t[ri],l=o[$n].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];Ss(e,n,r,u,s)}else{let c=l,u=o[Kt];_y(i)&&(c.flags|=128),Kf(n,e,c,u,r,s,!0)}}function RS(n,e,t,i,r){let s=t[Rr],o=oi(t);s!==o&&Ss(e,n,i,s,r);for(let a=En;a<t.length;a++){let l=t[a];Kl(l[We],l,n,e,i,s)}}function NS(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:xi.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=xi.Important),n.setStyle(t,i,r,s))}}function kl(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(oi(s)),Mi(s)&&PS(s,i);let o=t.type;if(o&8)kl(n,e,t.child,i);else if(o&32){let a=Yf(t,e),l;for(;l=a();)i.push(l)}else if(o&16){let a=Xy(e,t);if(Array.isArray(a))i.push(...a);else{let l=Nr(e[ri]);kl(l[We],l,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function PS(n,e){for(let t=En;t<n.length;t++){let i=n[t],r=i[We].firstChild;r!==null&&kl(i[We],i,r,e)}n[Rr]!==n[bi]&&e.push(n[Rr])}function Yy(n){if(n[md]!==null){for(let e of n[md])e.impl.addSequence(e);n[md].length=0}}var Zy=[];function OS(n){return n[Nn]??FS(n)}function FS(n){let e=Zy.pop()??Object.create(kS);return e.lView=n,e}function LS(n){n.lView[Nn]!==n&&(n.lView=null,Zy.push(n))}var kS=Ct(dt({},Co),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Yl(n.lView)},consumerOnSignalRead(){this.lView[Nn]=this}});function US(n){let e=n[Nn]??Object.create(VS);return e.lView=n,e}var VS=Ct(dt({},Co),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Nr(n.lView);for(;e&&!Jy(e[We]);)e=Nr(e);e&&Vv(e)},consumerOnSignalRead(){this.lView[Nn]=this}});function Jy(n){return n.type!==2}function Ky(n){if(n[wl]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[wl])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Le]&8192)}}var BS=100;function Qy(n,e=!0,t=0){let r=n[vi].rendererFactory,s=!1;s||r.begin?.();try{HS(n,t)}catch(o){throw e&&pS(n,o),o}finally{s||r.end?.()}}function HS(n,e){let t=Wv();try{Pg(!0),Yd(n,e);let i=0;for(;Xl(n);){if(i===BS)throw new De(103,!1);i++,Yd(n,1)}}finally{Pg(t)}}function zS(n,e,t,i){if(Os(e))return;let r=e[Le],s=!1,o=!1;If(e);let a=!0,l=null,c=null;s||(Jy(n)?(c=OS(e),l=Ua(c)):Bu()===null?(a=!1,c=US(e),l=Ua(c)):e[Nn]&&($u(e[Nn]),e[Nn]=null));try{Uv(e),VM(n.bindingStartIndex),t!==null&&Hy(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let f=n.preOrderCheckHooks;f!==null&&fl(e,f,null)}else{let f=n.preOrderHooks;f!==null&&hl(e,f,0,null),gd(e,0)}if(o||GS(e),Ky(e),e_(e,0),n.contentQueries!==null&&Ay(n,e),!s)if(u){let f=n.contentCheckHooks;f!==null&&fl(e,f)}else{let f=n.contentHooks;f!==null&&hl(e,f,1),gd(e,1)}jS(n,e);let d=n.components;d!==null&&n_(e,d,0);let h=n.viewQuery;if(h!==null&&$d(2,h,i),!s)if(u){let f=n.viewCheckHooks;f!==null&&fl(e,f)}else{let f=n.viewHooks;f!==null&&hl(e,f,2),gd(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[pd]){for(let f of e[pd])f();e[pd]=null}s||(Yy(e),e[Le]&=-73)}catch(u){throw s||Yl(e),u}finally{c!==null&&(Wu(c,l),a&&LS(c)),Rf()}}function e_(n,e){for(let t=My(n);t!==null;t=Ey(t))for(let i=En;i<t.length;i++){let r=t[i];t_(r,e)}}function GS(n){for(let e=My(n);e!==null;e=Ey(e)){if(!(e[Le]&2))continue;let t=e[Ds];for(let i=0;i<t.length;i++){let r=t[i];Vv(r)}}}function WS(n,e,t){_t(18);let i=ai(e,n);t_(i,t),_t(19,i[gn])}function t_(n,e){wf(n)&&Yd(n,e)}function Yd(n,e){let i=n[We],r=n[Le],s=n[Nn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&ju(s)),o||=!1,s&&(s.dirty=!1),n[Le]&=-9217,o)zS(i,n,i.template,n[gn]);else if(r&8192){Ky(n),e_(n,1);let a=i.components;a!==null&&n_(n,a,1),Yy(n)}}function n_(n,e,t){for(let i=0;i<e.length;i++)WS(n,e[i],t)}function jS(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Pr(~r);else{let s=r,o=t[++i],a=t[++i];HM(o,s);let l=e[s];_t(24,l),a(2,l),_t(25,l)}}}finally{Pr(-1)}}function Qf(n,e){let t=Wv()?64:1088;for(n[vi].changeDetectionScheduler?.notify(e);n;){n[Le]|=t;let i=Nr(n);if(Tl(n)&&!i)return n;n=i}return null}function i_(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function $S(n,e,t,i=!0){let r=e[We];if(qS(r,e,n,t),i){let o=Xd(t,n),a=e[jt],l=a.parentNode(n[Rr]);l!==null&&bS(r,n[$n],a,e,l,o)}let s=e[El];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function Zd(n,e){if(n.length<=En)return;let t=En+e,i=n[t];if(i){let r=i[Ir];r!==null&&r!==n&&Zf(r,i),e>0&&(n[t-1][jn]=i[jn]);let s=xl(n,En+e);xS(i[We],i);let o=s[yi];o!==null&&o.detachView(s[We]),i[Kt]=null,i[jn]=null,i[Le]&=-129}return i}function qS(n,e,t,i){let r=En+i,s=t.length;i>0&&(t[r-1][jn]=e),i<s-En?(e[jn]=t[r],xv(t,En+i,e)):(t.push(e),e[jn]=null),e[Kt]=t;let o=e[Ir];o!==null&&t!==o&&r_(o,e);let a=e[yi];a!==null&&a.insertView(n),Pd(e),e[Le]|=128}function r_(n,e){let t=n[Ds],i=e[Kt];if(Yi(i))n[Le]|=2;else{let r=i[Kt][ri];e[ri]!==r&&(n[Le]|=2)}t===null?n[Ds]=[e]:t.push(e)}var Bo=class{_lView;_cdRefInjectingView;notifyErrorHandler;_appRef=null;_attachedToViewContainer=!1;get rootNodes(){let e=this._lView,t=e[We];return kl(t,e,t.firstChild,[])}constructor(e,t,i=!0){this._lView=e,this._cdRefInjectingView=t,this.notifyErrorHandler=i}get context(){return this._lView[gn]}set context(e){this._lView[gn]=e}get destroyed(){return Os(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Kt];if(Mi(e)){let t=e[Cl],i=t?t.indexOf(this):-1;i>-1&&(Zd(e,i),xl(t,i))}this._attachedToViewContainer=!1}qy(this._lView[We],this._lView)}onDestroy(e){Bv(this._lView,e)}markForCheck(){Qf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Le]&=-129}reattach(){Pd(this._lView),this._lView[Le]|=128}detectChanges(){this._lView[Le]|=1024,Qy(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new De(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Tl(this._lView),t=this._lView[Ir];t!==null&&!e&&Zf(t,this._lView),$y(this._lView[We],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new De(902,!1);this._appRef=e;let t=Tl(this._lView),i=this._lView[Ir];i!==null&&!t&&r_(i,this._lView),Pd(this._lView)}};var Fr=(()=>{class n{static __NG_ELEMENT_ID__=ZS}return n})(),XS=Fr,YS=class extends XS{_declarationLView;_declarationTContainer;elementRef;constructor(e,t,i){super(),this._declarationLView=e,this._declarationTContainer=t,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,t){return this.createEmbeddedViewImpl(e,t)}createEmbeddedViewImpl(e,t,i){let r=yS(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:t,dehydratedView:i});return new Bo(r)}};function ZS(){return eh(On(),ft())}function eh(n,e){return n.type&4?new YS(e,n,Ls(n,e)):null}function th(n,e,t,i,r){let s=n.data[e];if(s===null)s=JS(n,e,t,i,r),BM()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=OM();s.injectorIndex=o===null?-1:o.injectorIndex}return jo(s,!0),s}function JS(n,e,t,i,r){let s=zv(),o=Gv(),a=o?s:s&&s.parent,l=n.data[e]=QS(n,a,t,e,i,r);return KS(n,l,s,o),l}function KS(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function QS(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return RM()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Qv(),attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var I2=new RegExp(`^(\\d+)*(${DE}|${TE})*(.*)`);var ew=()=>null;function Xg(n,e){return ew(n,e)}var tw=class{},s_=class{},Jd=class{resolveComponentFactory(e){throw Error(`No component factory found for ${Rn(e)}.`)}},nh=class{static NULL=new Jd},Is=class{},Zo=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>nw()}return n})();function nw(){let n=ft(),e=On(),t=ai(e.index,n);return(Yi(t)?t:n)[jt]}var iw=(()=>{class n{static \u0275prov=it({token:n,providedIn:"root",factory:()=>null})}return n})();var _d={},Kd=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){i=Wl(i);let r=this.injector.get(e,_d,i);return r!==_d||t===_d?r:this.parentInjector.get(e,t,i)}};function Yg(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=Sg(r,a);else if(s==2){let l=a,c=e[++o];i=Sg(i,l+": "+c+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function we(n,e=je.Default){let t=ft();if(t===null)return Ze(n,e);let i=On();return cy(i,t,sn(n),e)}function o_(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a,l=null,c=null,u=sw(o);u===null?a=o:[a,l,c]=u,lw(n,e,t,a,s,l,c)}s!==null&&i!==null&&rw(t,i,s)}function rw(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new De(-301,!1);i.push(e[r],s)}}function sw(n){let e=null,t=!1;for(let o=0;o<n.length;o++){let a=n[o];if(o===0&&si(a)&&(e=a),a.findHostDirectiveDefs!==null){t=!0;break}}if(!t)return null;let i=null,r=null,s=null;for(let o of n)o.findHostDirectiveDefs!==null&&(i??=[],r??=new Map,s??=new Map,ow(o,i,s,r)),o===e&&(i??=[],i.push(o));return i!==null?(i.push(...e===null?n:n.slice(1)),[i,r,s]):null}function ow(n,e,t,i){let r=e.length;n.findHostDirectiveDefs(n,e,i),t.set(n,[r,e.length-1])}function aw(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function lw(n,e,t,i,r,s,o){let a=i.length,l=!1;for(let h=0;h<a;h++){let f=i[h];!l&&si(f)&&(l=!0,aw(n,t,h)),kd(Nl(t,e),n,f.type)}pw(t,n.data.length,a);for(let h=0;h<a;h++){let f=i[h];f.providersResolver&&f.providersResolver(f)}let c=!1,u=!1,d=Vy(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let h=0;h<a;h++){let f=i[h];if(t.mergedAttrs=ko(t.mergedAttrs,f.hostAttrs),uw(n,t,e,d,f),hw(d,f,r),o!==null&&o.has(f)){let[v,m]=o.get(f);t.directiveToIndex.set(f.type,[d,v+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(f))&&t.directiveToIndex.set(f.type,d);f.contentQueries!==null&&(t.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(t.flags|=64);let g=f.type.prototype;!c&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),c=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}cw(n,t,s)}function cw(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Zg(0,e,r,i),Zg(1,e,r,i),Kg(e,i,!1);else{let s=t.get(r);Jg(0,e,s,i),Jg(1,e,s,i),Kg(e,i,!0)}}}function Zg(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),a_(e,s)}}function Jg(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),a_(e,o)}}function a_(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function Kg(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||zf(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!t&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===e){o??=[],o.push(l,i[a+1]);break}}else if(t&&s.hasOwnProperty(l)){let c=s[l];for(let u=0;u<c.length;u+=2)if(c[u]===e){o??=[],o.push(c[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function uw(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Dr(r.type,!0)),o=new Or(s,si(r),we);n.blueprint[i]=o,t[i]=o,dw(n,e,i,Vy(n,t,r.hostVars,wi),r)}function dw(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;fw(o)!=a&&o.push(a),o.push(t,i,s)}}function fw(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function hw(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;si(e)&&(t[""]=n)}}function pw(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function l_(n,e,t,i,r,s,o,a){let l=e.consts,c=Dl(l,o),u=th(e,n,2,i,c);return s&&o_(e,t,u,Dl(l,a),r),u.mergedAttrs=ko(u.mergedAttrs,u.attrs),u.attrs!==null&&Yg(u,u.attrs,!1),u.mergedAttrs!==null&&Yg(u,u.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,u),u}function c_(n,e){ty(n,e),Iv(e)&&n.queries.elementEnd(e)}var Qd=class extends nh{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Oo(e);return new Ul(t,this.ngModule)}};function mw(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&Jl.SignalBased)!==0};return r&&(s.transform=r),s})}function gw(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function vw(n,e,t){let i=e instanceof ii?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Kd(t,i):t}function yw(n){let e=n.get(Is,null);if(e===null)throw new De(407,!1);let t=n.get(iw,null),i=n.get(Vo,null);return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i}}function _w(n,e){let t=(n.selectors[0][0]||"div").toLowerCase();return Ny(e,t,t==="svg"?xM:t==="math"?bM:null)}var Ul=class extends s_{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=mw(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=gw(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=eS(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r){_t(22);let s=Je(null);try{let o=this.componentDef,a=i?["ng-version","19.2.25"]:tS(this.componentDef.selectors[0]),l=Gf(0,null,null,1,0,null,null,null,null,[a],null),c=vw(o,r||this.ngModule,e),u=yw(c),d=u.rendererFactory.createRenderer(null,o),h=i?sS(d,i,o.encapsulation,c):_w(o,d),f=Wf(null,l,null,512|Uy(o),null,null,u,d,c,null,Dy(h,c,!0));f[Pn]=h,If(f);let g=null;try{let v=l_(Pn,l,f,"#host",()=>[this.componentDef],!0,0);h&&(Oy(d,h,v),qo(h,f)),$f(l,f,v),Iy(l,v,f),c_(l,v),t!==void 0&&xw(v,this.ngContentSelectors,t),g=ai(v.index,f),f[gn]=g[gn],Xf(l,f,null)}catch(v){throw g!==null&&Wd(g),Wd(f),v}finally{_t(23),Rf()}return new ef(this.componentType,f)}finally{Je(s)}}},ef=class extends tw{_rootLView;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t){super(),this._rootLView=t,this._tNode=Lv(t[We],Pn),this.location=Ls(this._tNode,t),this.instance=ai(this._tNode.index,t)[gn],this.hostView=this.changeDetectorRef=new Bo(t,void 0,!1),this.componentType=e}setInput(e,t){let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=qf(i,r[We],r,e,t);this.previousInputValues.set(e,t);let o=ai(i.index,r);Qf(o,1)}get injector(){return new Tr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function xw(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var ks=(()=>{class n{static __NG_ELEMENT_ID__=bw}return n})();function bw(){let n=On();return d_(n,ft())}var Mw=ks,u_=class extends Mw{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return Ls(this._hostTNode,this._hostLView)}get injector(){return new Tr(this._hostTNode,this._hostLView)}get parentInjector(){let e=Of(this._hostTNode,this._hostLView);if(iy(e)){let t=Il(e,this._hostLView),i=Al(e),r=t[We].data[i+8];return new Tr(r,t)}else return new Tr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Qg(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-En}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=Xg(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,$g(this._hostTNode,o)),a}createComponent(e,t,i,r,s){let o=e&&!gM(e),a;if(o)a=t;else{let g=t||{};a=g.index,i=g.injector,r=g.projectableNodes,s=g.environmentInjector||g.ngModuleRef}let l=o?e:new Ul(Oo(e)),c=i||this.parentInjector;if(!s&&l.ngModule==null){let v=(o?c:this.parentInjector).get(ii,null);v&&(s=v)}let u=Oo(l.componentType??{}),d=Xg(this._lContainer,u?.id??null),h=d?.firstChild??null,f=l.create(c,r,h,s);return this.insertImpl(f.hostView,a,$g(this._hostTNode,d)),f}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(SM(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let l=r[Kt],c=new u_(l,l[$n],l[Kt]);c.detach(c.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return $S(o,r,s,i),e.attachToViewContainerRef(),xv(xd(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Qg(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=Zd(this._lContainer,t);i&&(xl(xd(this._lContainer),t),qy(i[We],i))}detach(e){let t=this._adjustIndex(e,-1),i=Zd(this._lContainer,t);return i&&xl(xd(this._lContainer),t)!=null?new Bo(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Qg(n){return n[Cl]}function xd(n){return n[Cl]||(n[Cl]=[])}function d_(n,e){let t,i=e[n.index];return Mi(i)?t=i:(t=i_(i,e,null,n),e[n.index]=t,jf(e,t)),Sw(t,e,n,i),new u_(t,n,e)}function Ew(n,e){let t=n[jt],i=t.createComment(""),r=Ei(e,n),s=t.parentNode(r);return Ll(t,s,i,t.nextSibling(r),!1),i}var Sw=Tw,ww=()=>!1;function Cw(n,e,t){return ww(n,e,t)}function Tw(n,e,t,i){if(n[Rr])return;let r;t.type&8?r=oi(i):r=Ew(e,t),n[Rr]=r}var tf=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},nf=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)ih(e,t).matches!==null&&this.queries[t].setDirty()}},rf=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=Fw(e):this.predicate=e}},sf=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},of=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,Dw(t,s)),this.matchTNodeWithReadOption(e,t,pl(t,e,s,!1,!1))}else i===Fr?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,pl(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===Si||r===ks||r===Fr&&t.type&4)this.addMatch(t.index,-2);else{let s=pl(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function Dw(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function Aw(n,e){return n.type&11?Ls(n,e):n.type&4?eh(n,e):null}function Iw(n,e,t,i){return t===-1?Aw(e,n):t===-2?Rw(n,e,i):Uo(n,n[We],t,e)}function Rw(n,e,t){if(t===Si)return Ls(e,n);if(t===Fr)return eh(e,n);if(t===ks)return d_(e,n)}function f_(n,e,t,i){let r=e[yi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let l=0;o!==null&&l<o.length;l+=2){let c=o[l];if(c<0)a.push(null);else{let u=s[c];a.push(Iw(e,u,o[l+1],t.metadata.read))}}r.matches=a}return r.matches}function af(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=f_(n,e,r,t);for(let a=0;a<s.length;a+=2){let l=s[a];if(l>0)i.push(o[a/2]);else{let c=s[a+1],u=e[-l];for(let d=En;d<u.length;d++){let h=u[d];h[Ir]===h[Kt]&&af(h[We],h,c,i)}if(u[Ds]!==null){let d=u[Ds];for(let h=0;h<d.length;h++){let f=d[h];af(f[We],f,c,i)}}}}}return i}function Nw(n,e){return n[yi].queries[e].queryList}function Pw(n,e,t){let i=new Gd((t&4)===4);return TM(n,e,i,i.destroy),(e[yi]??=new nf).queries.push(new tf(i))-1}function Ow(n,e,t){let i=on();return i.firstCreatePass&&(Lw(i,new rf(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),Pw(i,ft(),e)}function Fw(n){return n.split(",").map(e=>e.trim())}function Lw(n,e,t){n.queries===null&&(n.queries=new sf),n.queries.track(new of(e,t))}function ih(n,e){return n.queries.getByIndex(e)}function kw(n,e){let t=n[We],i=ih(t,e);return i.crossesNgTemplate?af(t,n,e,[]):f_(t,n,i,e)}var Vl=class{};var Bl=class extends Vl{injector;componentFactoryResolver=new Qd(this);instance=null;constructor(e){super();let t=new Fo([...e.providers,{provide:Vl,useValue:this},{provide:nh,useValue:this.componentFactoryResolver}],e.parent||Mf(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function Uw(n,e,t=null){return new Bl({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var Vw=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Sv(!1,t.type),r=i.length>0?Uw([i],this._injector,`Standalone[${t.type.name}]`):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=it({token:n,providedIn:"environment",factory:()=>new n(Ze(ii))})}return n})();function Vt(n){return Go(()=>{let e=p_(n),t=Ct(dt({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===xy.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(Vw).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||li.Emulated,styles:n.styles||In,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&IE("NgStandalone"),m_(t);let i=n.dependencies;return t.directiveDefs=ev(i,!1),t.pipeDefs=ev(i,!0),t.id=Ww(t),t})}function Bw(n){return Oo(n)||iM(n)}function Hw(n){return n!==null}function Hr(n){return Go(()=>({type:n.type,bootstrap:n.bootstrap||In,declarations:n.declarations||In,imports:n.imports||In,exports:n.exports||In,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function zw(n,e){if(n==null)return Ar;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,l;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,l=r[3]||null):(s=r,o=r,a=Jl.None,l=null),t[s]=[i,a,l],e[s]=o}return t}function Gw(n){if(n==null)return Ar;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function vn(n){return Go(()=>{let e=p_(n);return m_(e),e})}function h_(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function p_(n){let e={};return{type:n.type,providersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||Ar,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||In,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:zw(n.inputs,e),outputs:Gw(n.outputs),debugInfo:null}}function m_(n){n.features?.forEach(e=>e(n))}function ev(n,e){if(!n)return null;let t=e?rM:Bw;return()=>(typeof n=="function"?n():n).map(i=>t(i)).filter(Hw)}function Ww(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function jw(n){return Object.getPrototypeOf(n.prototype).constructor}function Ci(n){let e=jw(n.type),t=!0,i=[n];for(;e;){let r;if(si(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new De(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=bd(n.inputs),o.declaredInputs=bd(n.declaredInputs),o.outputs=bd(n.outputs);let a=r.hostBindings;a&&Zw(n,a);let l=r.viewQuery,c=r.contentQueries;if(l&&Xw(n,l),c&&Yw(n,c),$w(n,r),Ob(n.outputs,r.outputs),si(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===Ci&&(t=!1)}}e=Object.getPrototypeOf(e)}qw(i)}function $w(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t])}}function qw(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=ko(r.hostAttrs,t=ko(t,r.hostAttrs))}}function bd(n){return n===Ar?{}:n===In?[]:n}function Xw(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function Yw(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function Zw(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}function g_(n){return Kw(n)?Array.isArray(n)||!(n instanceof Map)&&Symbol.iterator in n:!1}function Jw(n,e){if(Array.isArray(n))for(let t=0;t<n.length;t++)e(n[t]);else{let t=n[Symbol.iterator](),i;for(;!(i=t.next()).done;)e(i.value)}}function Kw(n){return n!==null&&(typeof n=="function"||typeof n=="object")}function Qw(n,e,t){return n[e]=t}function Lr(n,e,t){let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function eC(n,e,t,i){let r=Lr(n,e,t);return Lr(n,e+1,i)||r}function tC(n,e,t,i,r,s,o,a,l){let c=e.consts,u=th(e,n,4,o||null,a||null);Hv()&&o_(e,t,u,Dl(c,l),Wy),u.mergedAttrs=ko(u.mergedAttrs,u.attrs),ty(e,u);let d=u.tView=Gf(2,u,i,r,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,c,null);return e.queries!==null&&(e.queries.template(e,u),d.queries=e.queries.embeddedTView(u)),u}function nC(n,e,t,i,r,s,o,a,l,c){let u=t+Pn,d=e.firstCreatePass?tC(u,e,n,i,r,s,o,a,l):e.data[u];jo(d,!1);let h=iC(e,n,d,t);Nf()&&Jf(e,n,h,d),qo(h,n);let f=i_(h,n,h,d);return n[u]=f,jf(n,f),Cw(f,d,n),Ef(d)&&$f(e,n,d),l!=null&&zy(n,d,c),d}function Tt(n,e,t,i,r,s,o,a){let l=ft(),c=on(),u=Dl(c.consts,s);return nC(l,c,n,e,t,i,r,u,o,a),Tt}var iC=rC;function rC(n,e,t,i){return Pf(!0),e[jt].createComment("")}var v_=new Ne("");var sC=(()=>{class n{static \u0275prov=it({token:n,providedIn:"root",factory:()=>new lf})}return n})(),lf=class{queuedEffectCount=0;queues=new Map;schedule(e){this.enqueue(e)}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),this.queuedEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||(this.queuedEffectCount++,i.add(e))}flush(){for(;this.queuedEffectCount>0;)for(let[e,t]of this.queues)e===null?this.flushQueue(t):e.run(()=>this.flushQueue(t))}flushQueue(e){for(let t of e)e.delete(t),this.queuedEffectCount--,t.run()}};function Jo(n){return!!n&&typeof n.then=="function"}function y_(n){return!!n&&typeof n.subscribe=="function"}var oC=new Ne("");var __=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=Ye(oC,{optional:!0})??[];injector=Ye(As);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=$l(this.injector,r);if(Jo(s))t.push(s);else if(y_(s)){let o=new Promise((a,l)=>{s.subscribe({complete:a,error:l})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),aC=new Ne("");function lC(){Xu(()=>{throw new De(600,!1)})}function cC(n){return n.isBoundToModule}var uC=10;var Ho=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=Ye(gE);afterRenderManager=Ye(RE);zonelessEnabled=Ye(hy);rootEffectScheduler=Ye(sC);dirtyFlags=0;tracingSnapshot=null;externalTestViews=new Set;afterTick=new fn;get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];isStable=Ye(Fs).hasPendingTasks.pipe(pn(t=>!t));constructor(){Ye(Zl,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=Ye(ii);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=As.NULL){_t(10);let s=t instanceof s_;if(!this._injector.get(__).done){let f="";throw new De(405,f)}let a;s?a=t:a=this._injector.get(nh).resolveComponentFactory(t),this.componentTypes.push(a.componentType);let l=cC(a)?void 0:this._injector.get(Vl),c=i||a.selector,u=a.create(r,[],c,l),d=u.location.nativeElement,h=u.injector.get(v_,null);return h?.registerApplication(d),u.onDestroy(()=>{this.detachView(u.hostView),ml(this.components,u),h?.unregisterApplication(d)}),this._loadComponent(u),_t(11,u),u}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){_t(12),this.tracingSnapshot!==null?this.tracingSnapshot.run(Ty.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw new De(101,!1);let t=Je(null);try{this._runningTick=!0,this.synchronize()}catch(i){this.internalErrorHandler(i)}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Je(t),this.afterTick.next(),_t(13)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Is,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<uC;)_t(14),this.synchronizeOnce(),_t(15)}synchronizeOnce(){if(this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush()),this.dirtyFlags&7){let t=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i,notifyErrorHandler:r}of this.allViews)dC(i,r,t,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}else this._rendererFactory?.begin?.(),this._rendererFactory?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Xl(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;ml(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView),this.tick(),this.components.push(t),this._injector.get(aC,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>ml(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new De(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function ml(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function dC(n,e,t,i){if(!t&&!Xl(n))return;Qy(n,e,t&&!i?0:1)}function fC(n,e,t,i){return Lr(n,Df(),t)?e+Po(t)+i:wi}function hC(n,e,t,i,r,s){let o=UM(),a=eC(n,o,t,r);return jv(2),a?e+Po(t)+i+Po(r)+s:wi}function ul(n,e){return n<<17|e<<2}function kr(n){return n>>17&32767}function pC(n){return(n&2)==2}function mC(n,e){return n&131071|e<<17}function cf(n){return n|2}function Rs(n){return(n&131068)>>2}function Md(n,e){return n&-131069|e<<2}function gC(n){return(n&1)===1}function uf(n){return n|1}function vC(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=kr(o),l=Rs(o);n[i]=t;let c=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Wo(d,u)>0)&&(c=!0)}else u=t;if(r)if(l!==0){let h=kr(n[a+1]);n[i+1]=ul(h,a),h!==0&&(n[h+1]=Md(n[h+1],i)),n[a+1]=mC(n[a+1],i)}else n[i+1]=ul(a,0),a!==0&&(n[a+1]=Md(n[a+1],i)),a=i;else n[i+1]=ul(l,0),a===0?a=i:n[l+1]=Md(n[l+1],i),l=i;c&&(n[i+1]=cf(n[i+1])),tv(n,u,i,!0),tv(n,u,i,!1),yC(e,u,n,i,s),o=ul(a,l),s?e.classBindings=o:e.styleBindings=o}function yC(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Wo(s,e)>=0&&(t[i+1]=uf(t[i+1]))}function tv(n,e,t,i){let r=n[t+1],s=e===null,o=i?kr(r):Rs(r),a=!1;for(;o!==0&&(a===!1||s);){let l=n[o],c=n[o+1];_C(l,e)&&(a=!0,n[o+1]=i?uf(c):cf(c)),o=i?kr(c):Rs(c)}a&&(n[t+1]=i?cf(r):uf(r))}function _C(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Wo(n,e)>=0:!1}function Oe(n,e,t){let i=ft(),r=Df();if(Lr(i,r,e)){let s=on(),o=Kv();Gy(s,o,i,n,e,i[jt],t,!1)}return Oe}function nv(n,e,t,i,r){qf(e,n,t,r?"class":"style",i)}function yn(n,e,t){return x_(n,e,t,!1),yn}function ht(n,e){return x_(n,e,null,!0),ht}function x_(n,e,t,i){let r=ft(),s=on(),o=jv(2);if(s.firstUpdatePass&&bC(s,n,o,i),e!==wi&&Lr(r,o,e)){let a=s.data[Br()];CC(s,a,r,r[jt],n,r[o+1]=TC(e,t),i,o)}}function xC(n,e){return e>=n.expandoStartIndex}function bC(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Br()],o=xC(n,t);DC(s,i)&&e===null&&!o&&(e=!1),e=MC(r,s,e,i),vC(r,s,e,t,o,i)}}function MC(n,e,t,i){let r=GM(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=Ed(null,n,e,t,i),t=zo(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=Ed(r,n,e,t,i),s===null){let l=EC(n,e,i);l!==void 0&&Array.isArray(l)&&(l=Ed(null,n,e,l[1],i),l=zo(l,e.attrs,i),SC(n,e,i,l))}else s=wC(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function EC(n,e,t){let i=t?e.classBindings:e.styleBindings;if(Rs(i)!==0)return n[kr(i)]}function SC(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[kr(r)]=i}function wC(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=zo(i,o,t)}return zo(i,e.attrs,t)}function Ed(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=zo(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function zo(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),tM(n,o,t?!0:e[++s]))}return n===void 0?null:n}function CC(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let l=n.data,c=l[a+1],u=gC(c)?iv(l,e,t,r,Rs(c),o):void 0;if(!Hl(u)){Hl(s)||pC(c)&&(s=iv(l,null,t,r,a,o));let d=Fv(Br(),t);NS(i,o,d,r,s)}}function iv(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let l=n[r],c=Array.isArray(l),u=c?l[1]:l,d=u===null,h=t[r+1];h===wi&&(h=d?In:void 0);let f=d?fd(h,i):u===i?h:void 0;if(c&&!Hl(f)&&(f=fd(l,i)),Hl(f)&&(a=f,o))return a;let g=n[r+1];r=o?kr(g):Rs(g)}if(e!==null){let l=s?e.residualClasses:e.residualStyles;l!=null&&(a=fd(l,i))}return a}function Hl(n){return n!==void 0}function TC(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=Rn(Ry(n)))),n}function DC(n,e){return(n.flags&(e?8:16))!==0}function L(n,e,t,i){let r=ft(),s=on(),o=Pn+n,a=r[jt],l=s.firstCreatePass?l_(o,s,r,e,Wy,Hv(),t,i):s.data[o],c=AC(s,r,l,a,e,n);r[o]=c;let u=Ef(l);return jo(l,!0),Oy(a,c,l),!jy(l)&&Nf()&&Jf(s,r,c,l),(DM()===0||u)&&qo(c,r),AM(),u&&($f(s,r,l),Iy(s,l,r)),i!==null&&zy(r,l),L}function U(){let n=On();Gv()?FM():(n=n.parent,jo(n,!1));let e=n;NM(e)&&PM(),IM();let t=on();return t.firstCreatePass&&c_(t,e),e.classesWithoutHost!=null&&XM(e)&&nv(t,e,ft(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&YM(e)&&nv(t,e,ft(),e.stylesWithoutHost,!1),U}function se(n,e,t,i){return L(n,e,t,i),U(),se}var AC=(n,e,t,i,r,s)=>(Pf(!0),Ny(i,r,Qv()));function Fn(){return ft()}var zl="en-US";var IC=zl;function RC(n){typeof n=="string"&&(IC=n.toLowerCase().replace(/_/g,"-"))}function rv(n,e,t){return function i(r){if(r===Function)return t;let s=Ps(n)?ai(n.index,e):e;Qf(s,5);let o=e[gn],a=sv(e,o,t,r),l=i.__ngNextListenerFn__;for(;l;)a=sv(e,o,l,r)&&a,l=l.__ngNextListenerFn__;return a}}function sv(n,e,t,i){let r=Je(null);try{return _t(6,e,t),t(i)!==!1}catch(s){return NC(n,s),!1}finally{_t(7,e,t),Je(r)}}function NC(n,e){let t=n[Ts],i=t?t.get(_i,null):null;i&&i.handleError(e)}function ov(n,e,t,i,r,s){let o=e[t],a=e[We],c=a.data[t].outputs[i],u=o[c],d=a.firstCreatePass?Tf(a):null,h=Cf(e),f=u.subscribe(s),g=h.length;h.push(s,f),d&&d.push(r,n.index,g,-(g+1))}function Se(n,e,t,i){let r=ft(),s=on(),o=On();return b_(s,r,r[jt],o,n,e,i),Se}function PC(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Sl],l=r[s+2];return a.length>l?a[l]:null}typeof o=="string"&&(s+=2)}return null}function b_(n,e,t,i,r,s,o){let a=Ef(i),c=n.firstCreatePass?Tf(n):null,u=Cf(e),d=!0;if(i.type&3||o){let h=Ei(i,e),f=o?o(h):h,g=u.length,v=o?p=>o(oi(p[i.index])):i.index,m=null;if(!o&&a&&(m=PC(n,e,r,i.index)),m!==null){let p=m.__ngLastListenerFn__||m;p.__ngNextListenerFn__=s,m.__ngLastListenerFn__=s,d=!1}else{s=rv(i,e,s),PE(e,f,r,s);let p=t.listen(f,r,s);u.push(s,p),c&&c.push(r,v,g,g+1)}}else s=rv(i,e,s);if(d){let h=i.outputs?.[r],f=i.hostDirectiveOutputs?.[r];if(f&&f.length)for(let g=0;g<f.length;g+=2){let v=f[g],m=f[g+1];ov(i,e,v,m,r,s)}if(h&&h.length)for(let g of h)ov(i,e,g,r,r,s)}}function kt(n=1){return jM(n)}function Zi(n,e,t){Ow(n,e,t)}function Ji(n){let e=ft(),t=on(),i=$v();Af(i+1);let r=ih(t,i);if(n.dirty&&EM(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=kw(e,i);n.reset(s,yE),n.notifyOnChanges()}return!0}return!1}function Ki(){return Nw(ft(),$v())}function M_(n){let e=LM();return kv(e,Pn+n)}function J(n,e=""){let t=ft(),i=on(),r=n+Pn,s=i.firstCreatePass?th(i,r,1,e,null):i.data[r],o=OC(i,t,s,e,n);t[r]=o,Nf()&&Jf(i,t,o,s),jo(s,!1)}var OC=(n,e,t,i,r)=>(Pf(!0),VE(e[jt],i));function Dt(n){return St("",n,""),Dt}function St(n,e,t){let i=ft(),r=fC(i,n,e,t);return r!==wi&&E_(i,Br(),r),St}function rh(n,e,t,i,r){let s=ft(),o=hC(s,n,e,t,i,r);return o!==wi&&E_(s,Br(),o),rh}function E_(n,e,t){let i=Fv(e,n);BE(n[jt],i,t)}function Qi(n,e,t){yy(e)&&(e=e());let i=ft(),r=Df();if(Lr(i,r,e)){let s=on(),o=Kv();Gy(s,o,i,n,e,i[jt],t,!1)}return Qi}function zr(n,e){let t=yy(n);return t&&n.set(e),t}function er(n,e){let t=ft(),i=on(),r=On();return b_(i,t,t[jt],r,n,e),er}function FC(n,e,t){let i=on();if(i.firstCreatePass){let r=si(n);df(t,i.data,i.blueprint,r,!0),df(e,i.data,i.blueprint,r,!1)}}function df(n,e,t,i,r){if(n=sn(n),Array.isArray(n))for(let s=0;s<n.length;s++)df(n[s],e,t,i,r);else{let s=on(),o=ft(),a=On(),l=Cs(n)?n:sn(n.provide),c=Tv(n),u=a.providerIndexes&1048575,d=a.directiveStart,h=a.providerIndexes>>20;if(Cs(n)||!n.multi){let f=new Or(c,r,we),g=wd(l,e,r?u:u+h,d);g===-1?(kd(Nl(a,o),s,l),Sd(s,n,e.length),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(f),o.push(f)):(t[g]=f,o[g]=f)}else{let f=wd(l,e,u+h,d),g=wd(l,e,u,u+h),v=f>=0&&t[f],m=g>=0&&t[g];if(r&&!m||!r&&!v){kd(Nl(a,o),s,l);let p=UC(r?kC:LC,t.length,r,i,c);!r&&m&&(t[g].providerFactory=p),Sd(s,n,e.length,0),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=S_(t[r?g:f],c,!r&&i);Sd(s,n,f>-1?f:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function Sd(n,e,t,i){let r=Cs(e),s=cM(e);if(r||s){let l=(s?sn(e.useClass):e).prototype.ngOnDestroy;if(l){let c=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=c.indexOf(t);u===-1?c.push(t,[i,l]):c[u+1].push(i,l)}else c.push(t,l)}}}function S_(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function wd(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function LC(n,e,t,i,r){return ff(this.multi,[])}function kC(n,e,t,i,r){let s=this.multi,o;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=Uo(i,i[We],this.providerFactory.index,r);o=l.slice(0,a),ff(s,o);for(let c=a;c<l.length;c++)o.push(l[c])}else o=[],ff(s,o);return o}function ff(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function UC(n,e,t,i,r){let s=new Or(n,t,we);return s.multi=[],s.index=e,s.componentProviders=0,S_(s,r,i&&!t),s}function Ko(n,e=[]){return t=>{t.providersResolver=(i,r)=>FC(i,r?r(n):n,e)}}function VC(n,e){let t=n[e];return t===wi?void 0:t}function BC(n,e,t,i,r,s){let o=e+t;return Lr(n,o,r)?Qw(n,o+1,s?i.call(s,r):i(r)):VC(n,o+1)}function Gr(n,e){let t=on(),i,r=n+Pn;t.firstCreatePass?(i=HC(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=Dr(i.type,!0)),o,a=mn(we);try{let l=Rl(!1),c=s();return Rl(l),MM(t,ft(),r,c),c}finally{mn(a)}}function HC(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function Wr(n,e,t){let i=n+Pn,r=ft(),s=kv(r,i);return zC(r,i)?BC(r,kM(),e,s.transform,t,s):s.transform(t)}function zC(n,e){return n[We].data[e].pure}var GC=(()=>{class n{zone=Ye(Jt);changeDetectionScheduler=Ye(Vo);applicationRef=Ye(Ho);_onMicrotaskEmptySubscription;initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function WC({ngZoneFactory:n,ignoreChangesOutsideZone:e,scheduleInRootZone:t}){return n??=()=>new Jt(Ct(dt({},jC()),{scheduleInRootZone:t})),[{provide:Jt,useFactory:n},{provide:bl,multi:!0,useFactory:()=>{let i=Ye(GC,{optional:!0});return()=>i.initialize()}},{provide:bl,multi:!0,useFactory:()=>{let i=Ye($C);return()=>{i.initialize()}}},e===!0?{provide:py,useValue:!0}:[],{provide:my,useValue:t??fy}]}function jC(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var $C=(()=>{class n{subscription=new rn;initialized=!1;zone=Ye(Jt);pendingTasks=Ye(Fs);initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{Jt.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{Jt.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var qC=(()=>{class n{appRef=Ye(Ho);taskService=Ye(Fs);ngZone=Ye(Jt);zonelessEnabled=Ye(hy);tracing=Ye(Zl,{optional:!0});disableScheduling=Ye(py,{optional:!0})??!1;zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new rn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ol):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(Ye(my,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof zd||!this.zoneIsDefined)}notify(t){if(!this.zonelessEnabled&&t===5)return;let i=!1;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2,i=!0;break}case 12:{this.appRef.dirtyFlags|=16,i=!0;break}case 13:{this.appRef.dirtyFlags|=2,i=!0;break}case 11:{i=!0;break}default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick(i))return;let r=this.useMicrotaskScheduler?Vg:gy;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(t){return!(this.disableScheduling&&!t||this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ol+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){throw this.taskService.remove(t),i}finally{this.cleanup()}this.useMicrotaskScheduler=!0,Vg(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function XC(){return typeof $localize<"u"&&$localize.locale||zl}var sh=new Ne("",{providedIn:"root",factory:()=>Ye(sh,je.Optional|je.SkipSelf)||XC()});var hf=new Ne(""),YC=new Ne("");function Ao(n){return!n.moduleRef}function ZC(n){let e=Ao(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Jt);return t.run(()=>{Ao(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(_i,null),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:s=>{i.handleError(s)}})}),Ao(n)){let s=()=>e.destroy(),o=n.platformInjector.get(hf);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(hf);o.add(s),n.moduleRef.onDestroy(()=>{ml(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return KC(i,t,()=>{let s=e.get(__);return s.runInitializers(),s.donePromise.then(()=>{let o=e.get(sh,zl);if(RC(o||zl),!e.get(YC,!0))return Ao(n)?e.get(Ho):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Ao(n)){let l=e.get(Ho);return n.rootComponent!==void 0&&l.bootstrap(n.rootComponent),l}else return JC(n.moduleRef,n.allPlatformModules),n.moduleRef})})})}function JC(n,e){let t=n.injector.get(Ho);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>t.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(t);else throw new De(-403,!1);e.push(n)}function KC(n,e,t){try{let i=t();return Jo(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n.handleError(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n.handleError(i)),i}}var gl=null;function QC(n=[],e){return As.create({name:e,providers:[{provide:jl,useValue:"platform"},{provide:hf,useValue:new Set([()=>gl=null])},...n]})}function eT(n=[]){if(gl)return gl;let e=QC(n);return gl=e,lC(),tT(e),e}function tT(n){let e=n.get(Bf,null);$l(n,()=>{e?.forEach(t=>t())})}var oh=(()=>{class n{static __NG_ELEMENT_ID__=nT}return n})();function nT(n){return iT(On(),ft(),(n&16)===16)}function iT(n,e,t){if(Ps(n)&&!t){let i=ai(n.index,e);return new Bo(i,i)}else if(n.type&175){let i=e[ri];return new Bo(i,e)}return null}var pf=class{constructor(){}supports(e){return g_(e)}create(e){return new mf(e)}},rT=(n,e)=>e,mf=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(e){this._trackByFn=e||rT}forEachItem(e){let t;for(t=this._itHead;t!==null;t=t._next)e(t)}forEachOperation(e){let t=this._itHead,i=this._removalsHead,r=0,s=null;for(;t||i;){let o=!i||t&&t.currentIndex<av(i,r,s)?t:i,a=av(o,r,s),l=o.currentIndex;if(o===i)r--,i=i._nextRemoved;else if(t=t._next,o.previousIndex==null)r++;else{s||(s=[]);let c=a-r,u=l-r;if(c!=u){for(let h=0;h<c;h++){let f=h<s.length?s[h]:s[h]=0,g=f+h;u<=g&&g<c&&(s[h]=f+1)}let d=o.previousIndex;s[d]=u-c}}a!==l&&e(o,a,l)}}forEachPreviousItem(e){let t;for(t=this._previousItHead;t!==null;t=t._nextPrevious)e(t)}forEachAddedItem(e){let t;for(t=this._additionsHead;t!==null;t=t._nextAdded)e(t)}forEachMovedItem(e){let t;for(t=this._movesHead;t!==null;t=t._nextMoved)e(t)}forEachRemovedItem(e){let t;for(t=this._removalsHead;t!==null;t=t._nextRemoved)e(t)}forEachIdentityChange(e){let t;for(t=this._identityChangesHead;t!==null;t=t._nextIdentityChange)e(t)}diff(e){if(e==null&&(e=[]),!g_(e))throw new De(900,!1);return this.check(e)?this:null}onDestroy(){}check(e){this._reset();let t=this._itHead,i=!1,r,s,o;if(Array.isArray(e)){this.length=e.length;for(let a=0;a<this.length;a++)s=e[a],o=this._trackByFn(a,s),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,s,o,a),i=!0):(i&&(t=this._verifyReinsertion(t,s,o,a)),Object.is(t.item,s)||this._addIdentityChange(t,s)),t=t._next}else r=0,Jw(e,a=>{o=this._trackByFn(r,a),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,a,o,r),i=!0):(i&&(t=this._verifyReinsertion(t,a,o,r)),Object.is(t.item,a)||this._addIdentityChange(t,a)),t=t._next,r++}),this.length=r;return this._truncate(t),this.collection=e,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let e;for(e=this._previousItHead=this._itHead;e!==null;e=e._next)e._nextPrevious=e._next;for(e=this._additionsHead;e!==null;e=e._nextAdded)e.previousIndex=e.currentIndex;for(this._additionsHead=this._additionsTail=null,e=this._movesHead;e!==null;e=e._nextMoved)e.previousIndex=e.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(e,t,i,r){let s;return e===null?s=this._itTail:(s=e._prev,this._remove(e)),e=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._reinsertAfter(e,s,r)):(e=this._linkedRecords===null?null:this._linkedRecords.get(i,r),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._moveAfter(e,s,r)):e=this._addAfter(new gf(t,i),s,r)),e}_verifyReinsertion(e,t,i,r){let s=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return s!==null?e=this._reinsertAfter(s,e._prev,r):e.currentIndex!=r&&(e.currentIndex=r,this._addToMoves(e,r)),e}_truncate(e){for(;e!==null;){let t=e._next;this._addToRemovals(this._unlink(e)),e=t}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(e,t,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(e);let r=e._prevRemoved,s=e._nextRemoved;return r===null?this._removalsHead=s:r._nextRemoved=s,s===null?this._removalsTail=r:s._prevRemoved=r,this._insertAfter(e,t,i),this._addToMoves(e,i),e}_moveAfter(e,t,i){return this._unlink(e),this._insertAfter(e,t,i),this._addToMoves(e,i),e}_addAfter(e,t,i){return this._insertAfter(e,t,i),this._additionsTail===null?this._additionsTail=this._additionsHead=e:this._additionsTail=this._additionsTail._nextAdded=e,e}_insertAfter(e,t,i){let r=t===null?this._itHead:t._next;return e._next=r,e._prev=t,r===null?this._itTail=e:r._prev=e,t===null?this._itHead=e:t._next=e,this._linkedRecords===null&&(this._linkedRecords=new Gl),this._linkedRecords.put(e),e.currentIndex=i,e}_remove(e){return this._addToRemovals(this._unlink(e))}_unlink(e){this._linkedRecords!==null&&this._linkedRecords.remove(e);let t=e._prev,i=e._next;return t===null?this._itHead=i:t._next=i,i===null?this._itTail=t:i._prev=t,e}_addToMoves(e,t){return e.previousIndex===t||(this._movesTail===null?this._movesTail=this._movesHead=e:this._movesTail=this._movesTail._nextMoved=e),e}_addToRemovals(e){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Gl),this._unlinkedRecords.put(e),e.currentIndex=null,e._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=e,e._prevRemoved=null):(e._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=e),e}_addIdentityChange(e,t){return e.item=t,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=e:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=e,e}},gf=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(e,t){this.item=e,this.trackById=t}},vf=class{_head=null;_tail=null;add(e){this._head===null?(this._head=this._tail=e,e._nextDup=null,e._prevDup=null):(this._tail._nextDup=e,e._prevDup=this._tail,e._nextDup=null,this._tail=e)}get(e,t){let i;for(i=this._head;i!==null;i=i._nextDup)if((t===null||t<=i.currentIndex)&&Object.is(i.trackById,e))return i;return null}remove(e){let t=e._prevDup,i=e._nextDup;return t===null?this._head=i:t._nextDup=i,i===null?this._tail=t:i._prevDup=t,this._head===null}},Gl=class{map=new Map;put(e){let t=e.trackById,i=this.map.get(t);i||(i=new vf,this.map.set(t,i)),i.add(e)}get(e,t){let i=e,r=this.map.get(i);return r?r.get(e,t):null}remove(e){let t=e.trackById;return this.map.get(t).remove(e)&&this.map.delete(t),e}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function av(n,e,t){let i=n.previousIndex;if(i===null)return i;let r=0;return t&&i<t.length&&(r=t[i]),i+e+r}function lv(){return new ah([new pf])}var ah=(()=>{class n{factories;static \u0275prov=it({token:n,providedIn:"root",factory:lv});constructor(t){this.factories=t}static create(t,i){if(i!=null){let r=i.factories.slice();t=t.concat(r)}return new n(t)}static extend(t){return{provide:n,useFactory:i=>n.create(t,i||lv()),deps:[[n,new Jb,new Zb]]}}find(t){let i=this.factories.find(r=>r.supports(t));if(i!=null)return i;throw new De(901,!1)}}return n})();function w_(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;_t(8);try{let s=r?.injector??eT(i),o=[WC({}),{provide:Vo,useExisting:qC},...t||[]],a=new Bl({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return ZC({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{_t(9)}}function C_(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function Ti(n){return Ku(n)}function Qo(n,e){return qu(n,e?.equal)}var cv=class{[zn];constructor(e){this[zn]=e}destroy(){this[zn].destroy()}};var Ln=new Ne("");var T_=null;function tr(){return T_}function lh(n){T_??=n}var ea=class{};var ch=/\s+/,D_=[],qn=(()=>{class n{_ngEl;_renderer;initialClasses=D_;rawClass;stateMap=new Map;constructor(t,i){this._ngEl=t,this._renderer=i}set klass(t){this.initialClasses=t!=null?t.trim().split(ch):D_}set ngClass(t){this.rawClass=typeof t=="string"?t.trim().split(ch):t}ngDoCheck(){for(let i of this.initialClasses)this._updateState(i,!0);let t=this.rawClass;if(Array.isArray(t)||t instanceof Set)for(let i of t)this._updateState(i,!0);else if(t!=null)for(let i of Object.keys(t))this._updateState(i,!!t[i]);this._applyStateDiff()}_updateState(t,i){let r=this.stateMap.get(t);r!==void 0?(r.enabled!==i&&(r.changed=!0,r.enabled=i),r.touched=!0):this.stateMap.set(t,{enabled:i,changed:!0,touched:!0})}_applyStateDiff(){for(let t of this.stateMap){let i=t[0],r=t[1];r.changed?(this._toggleClass(i,r.enabled),r.changed=!1):r.touched||(r.enabled&&this._toggleClass(i,!1),this.stateMap.delete(i)),r.touched=!1}}_toggleClass(t,i){t=t.trim(),t.length>0&&t.split(ch).forEach(r=>{i?this._renderer.addClass(this._ngEl.nativeElement,r):this._renderer.removeClass(this._ngEl.nativeElement,r)})}static \u0275fac=function(i){return new(i||n)(we(Si),we(Zo))};static \u0275dir=vn({type:n,selectors:[["","ngClass",""]],inputs:{klass:[0,"class","klass"],ngClass:"ngClass"}})}return n})();var Ql=class{$implicit;ngForOf;index;count;constructor(e,t,i,r){this.$implicit=e,this.ngForOf=t,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},kn=(()=>{class n{_viewContainer;_template;_differs;set ngForOf(t){this._ngForOf=t,this._ngForOfDirty=!0}set ngForTrackBy(t){this._trackByFn=t}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(t,i,r){this._viewContainer=t,this._template=i,this._differs=r}set ngForTemplate(t){t&&(this._template=t)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let t=this._ngForOf;!this._differ&&t&&(this._differ=this._differs.find(t).create(this.ngForTrackBy))}if(this._differ){let t=this._differ.diff(this._ngForOf);t&&this._applyChanges(t)}}_applyChanges(t){let i=this._viewContainer;t.forEachOperation((r,s,o)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Ql(r.item,this._ngForOf,-1,-1),o===null?void 0:o);else if(o==null)i.remove(s===null?void 0:s);else if(s!==null){let a=i.get(s);i.move(a,o),A_(a,r)}});for(let r=0,s=i.length;r<s;r++){let a=i.get(r).context;a.index=r,a.count=s,a.ngForOf=this._ngForOf}t.forEachIdentityChange(r=>{let s=i.get(r.currentIndex);A_(s,r)})}static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(we(ks),we(Fr),we(ah))};static \u0275dir=vn({type:n,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return n})();function A_(n,e){n.context.$implicit=e.item}var Xn=(()=>{class n{_viewContainer;_context=new ec;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(t,i){this._viewContainer=t,this._thenTemplateRef=i}set ngIf(t){this._context.$implicit=this._context.ngIf=t,this._updateView()}set ngIfThen(t){I_(t,!1),this._thenTemplateRef=t,this._thenViewRef=null,this._updateView()}set ngIfElse(t){I_(t,!1),this._elseTemplateRef=t,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(we(ks),we(Fr))};static \u0275dir=vn({type:n,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return n})(),ec=class{$implicit=null;ngIf=null};function I_(n,e){if(n&&!n.createEmbeddedView)throw new De(2020,!1)}function sT(n,e){return new De(2100,!1)}var jr=(()=>{class n{transform(t){if(t==null)return null;if(typeof t!="string")throw sT(n,t);return t.toUpperCase()}static \u0275fac=function(i){return new(i||n)};static \u0275pipe=h_({name:"uppercase",type:n,pure:!0})}return n})();var Ot=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Hr({type:n});static \u0275inj=Vr({})}return n})();function ta(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var uh="browser",R_="server";function tc(n){return n===R_}var $r=class{};var rc=new Ne(""),ph=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(r=>{r.manager=this}),this._plugins=t.slice().reverse()}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new De(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(Ze(rc),Ze(Jt))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})(),na=class{_doc;constructor(e){this._doc=e}manager},nc="ng-app-id";function N_(n){for(let e of n)e.remove()}function P_(n,e){let t=e.createElement("style");return t.textContent=n,t}function oT(n,e,t,i){let r=n.head?.querySelectorAll(`style[${nc}="${e}"],link[${nc}="${e}"]`);if(r)for(let s of r)s.removeAttribute(nc),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function fh(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var mh=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;isServer;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,this.isServer=tc(s),oT(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,P_);i?.forEach(r=>this.addUsage(r,this.external,fh))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(N_(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])N_(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,P_(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,fh(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),this.isServer&&i.setAttribute(nc,this.appId),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(Ze(Ln),Ze(Vf),Ze(Hf,8),Ze(Xo))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})(),dh={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},gh=/%COMP%/g;var F_="%COMP%",aT=`_nghost-${F_}`,lT=`_ngcontent-${F_}`,cT=!0,uT=new Ne("",{providedIn:"root",factory:()=>cT});function dT(n){return lT.replace(gh,n)}function fT(n){return aT.replace(gh,n)}function L_(n,e){return e.map(t=>t.replace(gh,n))}var vh=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;platformId;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;platformIsServer;constructor(t,i,r,s,o,a,l,c=null,u=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.platformId=a,this.ngZone=l,this.nonce=c,this.tracingService=u,this.platformIsServer=tc(a),this.defaultRenderer=new ia(t,o,l,this.platformIsServer,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;this.platformIsServer&&i.encapsulation===li.ShadowDom&&(i=Ct(dt({},i),{encapsulation:li.Emulated}));let r=this.getOrCreateRenderer(t,i);return r instanceof ic?r.applyToHost(t):r instanceof ra&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.platformIsServer,h=this.tracingService;switch(i.encapsulation){case li.Emulated:s=new ic(l,c,i,this.appId,u,o,a,d,h);break;case li.ShadowDom:return new hh(l,c,t,i,o,a,this.nonce,d,h);default:s=new ra(l,c,i,u,o,a,d,h);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(Ze(ph),Ze(mh),Ze(Vf),Ze(uT),Ze(Ln),Ze(Xo),Ze(Jt),Ze(Hf),Ze(Zl,8))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})(),ia=class{eventManager;doc;ngZone;platformIsServer;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r,s){this.eventManager=e,this.doc=t,this.ngZone=i,this.platformIsServer=r,this.tracingService=s}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(dh[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(O_(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(O_(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new De(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=dh[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=dh[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(xi.DashCase|xi.Important)?e.style.setProperty(t,i,r&xi.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&xi.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=tr().getGlobalEventTarget(this.doc,e),!e))throw new De(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;(this.platformIsServer?this.ngZone.runGuarded(()=>e(t)):e(t))===!1&&t.preventDefault()}}};function O_(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var hh=class extends ia{sharedStylesHost;hostEl;shadowRoot;constructor(e,t,i,r,s,o,a,l,c){super(e,s,o,l,c),this.sharedStylesHost=t,this.hostEl=i,this.shadowRoot=i.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let u=r.styles;u=L_(r.id,u);for(let h of u){let f=document.createElement("style");a&&f.setAttribute("nonce",a),f.textContent=h,this.shadowRoot.appendChild(f)}let d=r.getExternalStyles?.();if(d)for(let h of d){let f=fh(h,s);a&&f.setAttribute("nonce",a),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},ra=class extends ia{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,l,c){super(e,s,o,a,l),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let u=i.styles;this.styles=c?L_(c,u):u,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},ic=class extends ra{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,l,c){let u=r+"-"+i.id;super(e,t,i,s,o,a,l,c,u),this.contentAttr=dT(u),this.hostAttr=fT(u)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var sc=class n extends ea{supportsDOMEvents=!0;static makeCurrent(){lh(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=hT();return t==null?null:pT(t)}resetBaseElement(){sa=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return ta(document.cookie,e)}},sa=null;function hT(){return sa=sa||document.head.querySelector("base"),sa?sa.getAttribute("href"):null}function pT(n){return new URL(n,document.baseURI).pathname}var mT=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})(),U_=(()=>{class n extends na{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(Ze(Ln))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})(),k_=["alt","control","meta","shift"],gT={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},vT={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},V_=(()=>{class n extends na{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>tr().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),k_.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),o+=c+".")}),o+=s,i.length!=0||s.length===0)return null;let l={};return l.domEventName=r,l.fullKey=o,l}static matchEventFullKeyCode(t,i){let r=gT[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),k_.forEach(o=>{if(o!==r){let a=vT[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(Ze(Ln))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})();function yh(n,e,t){return w_(dt({rootComponent:n,platformRef:t?.platformRef},yT(e)))}function yT(n){return{appProviders:[...ET,...n?.providers??[]],platformProviders:MT}}function _T(){sc.makeCurrent()}function xT(){return new _i}function bT(){return wy(document),document}var MT=[{provide:Xo,useValue:uh},{provide:Bf,useValue:_T,multi:!0},{provide:Ln,useFactory:bT}];var ET=[{provide:jl,useValue:"root"},{provide:_i,useFactory:xT},{provide:rc,useClass:U_,multi:!0,deps:[Ln]},{provide:rc,useClass:V_,multi:!0,deps:[Ln]},vh,mh,ph,{provide:Is,useExisting:vh},{provide:$r,useClass:mT},[]];var Vs=class{},oa=class{},ir=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(e){e?typeof e=="string"?this.lazyInit=()=>{this.headers=new Map,e.split(`
`).forEach(t=>{let i=t.indexOf(":");if(i>0){let r=t.slice(0,i),s=t.slice(i+1).trim();this.addHeaderEntry(r,s)}})}:typeof Headers<"u"&&e instanceof Headers?(this.headers=new Map,e.forEach((t,i)=>{this.addHeaderEntry(i,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(e).forEach(([t,i])=>{this.setHeaderEntries(t,i)})}:this.headers=new Map}has(e){return this.init(),this.headers.has(e.toLowerCase())}get(e){this.init();let t=this.headers.get(e.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(e){return this.init(),this.headers.get(e.toLowerCase())||null}append(e,t){return this.clone({name:e,value:t,op:"a"})}set(e,t){return this.clone({name:e,value:t,op:"s"})}delete(e,t){return this.clone({name:e,value:t,op:"d"})}maybeSetNormalizedName(e,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,e)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(e=>this.applyUpdate(e)),this.lazyUpdate=null))}copyFrom(e){e.init(),Array.from(e.headers.keys()).forEach(t=>{this.headers.set(t,e.headers.get(t)),this.normalizedNames.set(t,e.normalizedNames.get(t))})}clone(e){let t=new n;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([e]),t}applyUpdate(e){let t=e.name.toLowerCase();switch(e.op){case"a":case"s":let i=e.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(e.name,t);let r=(e.op==="a"?this.headers.get(t):void 0)||[];r.push(...i),this.headers.set(t,r);break;case"d":let s=e.value;if(!s)this.headers.delete(t),this.normalizedNames.delete(t);else{let o=this.headers.get(t);if(!o)return;o=o.filter(a=>s.indexOf(a)===-1),o.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,o)}break}}addHeaderEntry(e,t){let i=e.toLowerCase();this.maybeSetNormalizedName(e,i),this.headers.has(i)?this.headers.get(i).push(t):this.headers.set(i,[t])}setHeaderEntries(e,t){let i=(Array.isArray(t)?t:[t]).map(s=>s.toString()),r=e.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(e,r)}forEach(e){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>e(this.normalizedNames.get(t),this.headers.get(t)))}};var ac=class{encodeKey(e){return B_(e)}encodeValue(e){return B_(e)}decodeKey(e){return decodeURIComponent(e)}decodeValue(e){return decodeURIComponent(e)}};function ST(n,e){let t=new Map;return n.length>0&&n.replace(/^\?/,"").split("&").forEach(r=>{let s=r.indexOf("="),[o,a]=s==-1?[e.decodeKey(r),""]:[e.decodeKey(r.slice(0,s)),e.decodeValue(r.slice(s+1))],l=t.get(o)||[];l.push(a),t.set(o,l)}),t}var wT=/%(\d[a-f0-9])/gi,CT={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function B_(n){return encodeURIComponent(n).replace(wT,(e,t)=>CT[t]??e)}function oc(n){return`${n}`}var Di=class n{map;encoder;updates=null;cloneFrom=null;constructor(e={}){if(this.encoder=e.encoder||new ac,e.fromString){if(e.fromObject)throw new De(2805,!1);this.map=ST(e.fromString,this.encoder)}else e.fromObject?(this.map=new Map,Object.keys(e.fromObject).forEach(t=>{let i=e.fromObject[t],r=Array.isArray(i)?i.map(oc):[oc(i)];this.map.set(t,r)})):this.map=null}has(e){return this.init(),this.map.has(e)}get(e){this.init();let t=this.map.get(e);return t?t[0]:null}getAll(e){return this.init(),this.map.get(e)||null}keys(){return this.init(),Array.from(this.map.keys())}append(e,t){return this.clone({param:e,value:t,op:"a"})}appendAll(e){let t=[];return Object.keys(e).forEach(i=>{let r=e[i];Array.isArray(r)?r.forEach(s=>{t.push({param:i,value:s,op:"a"})}):t.push({param:i,value:r,op:"a"})}),this.clone(t)}set(e,t){return this.clone({param:e,value:t,op:"s"})}delete(e,t){return this.clone({param:e,value:t,op:"d"})}toString(){return this.init(),this.keys().map(e=>{let t=this.encoder.encodeKey(e);return this.map.get(e).map(i=>t+"="+this.encoder.encodeValue(i)).join("&")}).filter(e=>e!=="").join("&")}clone(e){let t=new n({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(e),t}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(e=>this.map.set(e,this.cloneFrom.map.get(e))),this.updates.forEach(e=>{switch(e.op){case"a":case"s":let t=(e.op==="a"?this.map.get(e.param):void 0)||[];t.push(oc(e.value)),this.map.set(e.param,t);break;case"d":if(e.value!==void 0){let i=this.map.get(e.param)||[],r=i.indexOf(oc(e.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(e.param,i):this.map.delete(e.param)}else{this.map.delete(e.param);break}}}),this.cloneFrom=this.updates=null)}};var lc=class{map=new Map;set(e,t){return this.map.set(e,t),this}get(e){return this.map.has(e)||this.map.set(e,e.defaultValue()),this.map.get(e)}delete(e){return this.map.delete(e),this}has(e){return this.map.has(e)}keys(){return this.map.keys()}};function TT(n){switch(n){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function H_(n){return typeof ArrayBuffer<"u"&&n instanceof ArrayBuffer}function z_(n){return typeof Blob<"u"&&n instanceof Blob}function G_(n){return typeof FormData<"u"&&n instanceof FormData}function DT(n){return typeof URLSearchParams<"u"&&n instanceof URLSearchParams}var W_="Content-Type",j_="Accept",$_="X-Request-URL",q_="text/plain",X_="application/json",AT=`${X_}, ${q_}, */*`,Us=class n{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;responseType="json";method;params;urlWithParams;transferCache;constructor(e,t,i,r){this.url=t,this.method=e.toUpperCase();let s;if(TT(this.method)||r?(this.body=i!==void 0?i:null,s=r):s=i,s&&(this.reportProgress=!!s.reportProgress,this.withCredentials=!!s.withCredentials,s.responseType&&(this.responseType=s.responseType),s.headers&&(this.headers=s.headers),s.context&&(this.context=s.context),s.params&&(this.params=s.params),this.transferCache=s.transferCache),this.headers??=new ir,this.context??=new lc,!this.params)this.params=new Di,this.urlWithParams=t;else{let o=this.params.toString();if(o.length===0)this.urlWithParams=t;else{let a=t.indexOf("?"),l=a===-1?"?":a<t.length-1?"&":"";this.urlWithParams=t+l+o}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||H_(this.body)||z_(this.body)||G_(this.body)||DT(this.body)?this.body:this.body instanceof Di?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||G_(this.body)?null:z_(this.body)?this.body.type||null:H_(this.body)?null:typeof this.body=="string"?q_:this.body instanceof Di?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?X_:null}clone(e={}){let t=e.method||this.method,i=e.url||this.url,r=e.responseType||this.responseType,s=e.transferCache??this.transferCache,o=e.body!==void 0?e.body:this.body,a=e.withCredentials??this.withCredentials,l=e.reportProgress??this.reportProgress,c=e.headers||this.headers,u=e.params||this.params,d=e.context??this.context;return e.setHeaders!==void 0&&(c=Object.keys(e.setHeaders).reduce((h,f)=>h.set(f,e.setHeaders[f]),c)),e.setParams&&(u=Object.keys(e.setParams).reduce((h,f)=>h.set(f,e.setParams[f]),u)),new n(t,i,o,{params:u,headers:c,context:d,reportProgress:l,responseType:r,withCredentials:a,transferCache:s})}},qr=(function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n})(qr||{}),Bs=class{headers;status;statusText;url;ok;type;constructor(e,t=200,i="OK"){this.headers=e.headers||new ir,this.status=e.status!==void 0?e.status:t,this.statusText=e.statusText||i,this.url=e.url||null,this.ok=this.status>=200&&this.status<300}},cc=class n extends Bs{constructor(e={}){super(e)}type=qr.ResponseHeader;clone(e={}){return new n({headers:e.headers||this.headers,status:e.status!==void 0?e.status:this.status,statusText:e.statusText||this.statusText,url:e.url||this.url||void 0})}},aa=class n extends Bs{body;constructor(e={}){super(e),this.body=e.body!==void 0?e.body:null}type=qr.Response;clone(e={}){return new n({body:e.body!==void 0?e.body:this.body,headers:e.headers||this.headers,status:e.status!==void 0?e.status:this.status,statusText:e.statusText||this.statusText,url:e.url||this.url||void 0})}},la=class extends Bs{name="HttpErrorResponse";message;error;ok=!1;constructor(e){super(e,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${e.url||"(unknown url)"}`:this.message=`Http failure response for ${e.url||"(unknown url)"}: ${e.status} ${e.statusText}`,this.error=e.error||null}},IT=200,RT=204;function _h(n,e){return{body:e,headers:n.headers,context:n.context,observe:n.observe,params:n.params,reportProgress:n.reportProgress,responseType:n.responseType,withCredentials:n.withCredentials,transferCache:n.transferCache}}var dc=(()=>{class n{handler;constructor(t){this.handler=t}request(t,i,r={}){let s;if(t instanceof Us)s=t;else{let l;r.headers instanceof ir?l=r.headers:l=new ir(r.headers);let c;r.params&&(r.params instanceof Di?c=r.params:c=new Di({fromObject:r.params})),s=new Us(t,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache})}let o=al(s).pipe(cd(l=>this.handler.handle(l)));if(t instanceof Us||r.observe==="events")return o;let a=o.pipe(ld(l=>l instanceof aa));switch(r.observe||"body"){case"body":switch(s.responseType){case"arraybuffer":return a.pipe(pn(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new De(2806,!1);return l.body}));case"blob":return a.pipe(pn(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new De(2807,!1);return l.body}));case"text":return a.pipe(pn(l=>{if(l.body!==null&&typeof l.body!="string")throw new De(2808,!1);return l.body}));default:return a.pipe(pn(l=>l.body))}case"response":return a;default:throw new De(2809,!1)}}delete(t,i={}){return this.request("DELETE",t,i)}get(t,i={}){return this.request("GET",t,i)}head(t,i={}){return this.request("HEAD",t,i)}jsonp(t,i){return this.request("JSONP",t,{params:new Di().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,i={}){return this.request("OPTIONS",t,i)}patch(t,i,r={}){return this.request("PATCH",t,_h(r,i))}post(t,i,r={}){return this.request("POST",t,_h(r,i))}put(t,i,r={}){return this.request("PUT",t,_h(r,i))}static \u0275fac=function(i){return new(i||n)(Ze(Vs))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})();var NT=new Ne("");function PT(n,e){return e(n)}function OT(n,e,t){return(i,r)=>$l(t,()=>e(i,s=>n(s,r)))}var Y_=new Ne(""),Z_=new Ne(""),J_=new Ne("",{providedIn:"root",factory:()=>!0});var uc=(()=>{class n extends Vs{backend;injector;chain=null;pendingTasks=Ye(Fs);contributeToStability=Ye(J_);constructor(t,i){super(),this.backend=t,this.injector=i}handle(t){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Y_),...this.injector.get(Z_,[])]));this.chain=i.reduceRight((r,s)=>OT(r,s,this.injector),PT)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(t,r=>this.backend.handle(r)).pipe(ud(()=>this.pendingTasks.remove(i)))}else return this.chain(t,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||n)(Ze(oa),Ze(ii))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})();var FT=/^\)\]\}',?\n/,LT=RegExp(`^${$_}:`,"m");function kT(n){return"responseURL"in n&&n.responseURL?n.responseURL:LT.test(n.getAllResponseHeaders())?n.getResponseHeader($_):null}var xh=(()=>{class n{xhrFactory;constructor(t){this.xhrFactory=t}handle(t){if(t.method==="JSONP")throw new De(-2800,!1);let i=this.xhrFactory;return(i.\u0275loadImpl?wr(i.\u0275loadImpl()):al(null)).pipe(dd(()=>new Lt(s=>{let o=i.build();if(o.open(t.method,t.urlWithParams),t.withCredentials&&(o.withCredentials=!0),t.headers.forEach((v,m)=>o.setRequestHeader(v,m.join(","))),t.headers.has(j_)||o.setRequestHeader(j_,AT),!t.headers.has(W_)){let v=t.detectContentTypeHeader();v!==null&&o.setRequestHeader(W_,v)}if(t.responseType){let v=t.responseType.toLowerCase();o.responseType=v!=="json"?v:"text"}let a=t.serializeBody(),l=null,c=()=>{if(l!==null)return l;let v=o.statusText||"OK",m=new ir(o.getAllResponseHeaders()),p=kT(o)||t.url;return l=new cc({headers:m,status:o.status,statusText:v,url:p}),l},u=()=>{let{headers:v,status:m,statusText:p,url:S}=c(),E=null;m!==RT&&(E=typeof o.response>"u"?o.responseText:o.response),m===0&&(m=E?IT:0);let b=m>=200&&m<300;if(t.responseType==="json"&&typeof E=="string"){let F=E;E=E.replace(FT,"");try{E=E!==""?JSON.parse(E):null}catch(T){E=F,b&&(b=!1,E={error:T,text:E})}}b?(s.next(new aa({body:E,headers:v,status:m,statusText:p,url:S||void 0})),s.complete()):s.error(new la({error:E,headers:v,status:m,statusText:p,url:S||void 0}))},d=v=>{let{url:m}=c(),p=new la({error:v,status:o.status||0,statusText:o.statusText||"Unknown Error",url:m||void 0});s.error(p)},h=!1,f=v=>{h||(s.next(c()),h=!0);let m={type:qr.DownloadProgress,loaded:v.loaded};v.lengthComputable&&(m.total=v.total),t.responseType==="text"&&o.responseText&&(m.partialText=o.responseText),s.next(m)},g=v=>{let m={type:qr.UploadProgress,loaded:v.loaded};v.lengthComputable&&(m.total=v.total),s.next(m)};return o.addEventListener("load",u),o.addEventListener("error",d),o.addEventListener("timeout",d),o.addEventListener("abort",d),t.reportProgress&&(o.addEventListener("progress",f),a!==null&&o.upload&&o.upload.addEventListener("progress",g)),o.send(a),s.next({type:qr.Sent}),()=>{o.removeEventListener("error",d),o.removeEventListener("abort",d),o.removeEventListener("load",u),o.removeEventListener("timeout",d),t.reportProgress&&(o.removeEventListener("progress",f),a!==null&&o.upload&&o.upload.removeEventListener("progress",g)),o.readyState!==o.DONE&&o.abort()}})))}static \u0275fac=function(i){return new(i||n)(Ze($r))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})(),K_=new Ne(""),UT="XSRF-TOKEN",VT=new Ne("",{providedIn:"root",factory:()=>UT}),BT="X-XSRF-TOKEN",HT=new Ne("",{providedIn:"root",factory:()=>BT}),ca=class{},zT=(()=>{class n{doc;cookieName;lastCookieString="";lastToken=null;parseCount=0;constructor(t,i){this.doc=t,this.cookieName=i}getToken(){let t=this.doc.cookie||"";return t!==this.lastCookieString&&(this.parseCount++,this.lastToken=ta(t,this.cookieName),this.lastCookieString=t),this.lastToken}static \u0275fac=function(i){return new(i||n)(Ze(Ln),Ze(VT))};static \u0275prov=it({token:n,factory:n.\u0275fac})}return n})(),GT=/^(?:https?:)?\/\//i;function WT(n,e){if(!Ye(K_)||n.method==="GET"||n.method==="HEAD"||GT.test(n.url))return e(n);let t=Ye(ca).getToken(),i=Ye(HT);return t!=null&&!n.headers.has(i)&&(n=n.clone({headers:n.headers.set(i,t)})),e(n)}function bh(...n){let e=[dc,xh,uc,{provide:Vs,useExisting:uc},{provide:oa,useFactory:()=>Ye(NT,{optional:!0})??Ye(xh)},{provide:Y_,useValue:WT,multi:!0},{provide:K_,useValue:!0},{provide:ca,useClass:zT}];for(let t of n)e.push(...t.\u0275providers);return Ev(e)}var Bt=class n{constructor(){this.speechEnabled=!0;this.isListening=!1;this.speechResult$=new fn;this.audioIntensity$=new fn;this.avatarState$=new fn;this.recognition=null;this.audioCtx=null;this.currentAudioElement=null;this.pulseInterval=null;this.initAudioContext(),this.initSpeechRecognition()}initAudioContext(){try{let e=window.AudioContext||window.webkitAudioContext;e&&(this.audioCtx=new e)}catch{}}playSciFiTone(e="beep"){if(this.audioCtx)try{this.audioCtx.state==="suspended"&&this.audioCtx.resume();let t=this.audioCtx.createOscillator(),i=this.audioCtx.createGain();t.connect(i),i.connect(this.audioCtx.destination);let r=this.audioCtx.currentTime;e==="boot"?(t.type="sine",t.frequency.setValueAtTime(440,r),t.frequency.exponentialRampToValueAtTime(880,r+.2),i.gain.setValueAtTime(.15,r),i.gain.linearRampToValueAtTime(.01,r+.3),t.start(r),t.stop(r+.3)):e==="ack"?(t.type="sine",t.frequency.setValueAtTime(600,r),t.frequency.setValueAtTime(900,r+.08),i.gain.setValueAtTime(.1,r),i.gain.linearRampToValueAtTime(.01,r+.18),t.start(r),t.stop(r+.18)):e==="error"?(t.type="sawtooth",t.frequency.setValueAtTime(250,r),t.frequency.linearRampToValueAtTime(150,r+.2),i.gain.setValueAtTime(.12,r),i.gain.linearRampToValueAtTime(.01,r+.25),t.start(r),t.stop(r+.25)):(t.type="sine",t.frequency.setValueAtTime(750,r),i.gain.setValueAtTime(.08,r),i.gain.linearRampToValueAtTime(.01,r+.08),t.start(r),t.stop(r+.08))}catch{}}initSpeechRecognition(){let e=window.SpeechRecognition||window.webkitSpeechRecognition;e&&(this.recognition=new e,this.recognition.continuous=!1,this.recognition.interimResults=!1,this.recognition.lang="en-US",this.recognition.onstart=()=>{this.isListening=!0,this.playSciFiTone("ack"),this.avatarState$.next("listening")},this.recognition.onresult=t=>{let i=t.results[0][0].transcript;this.speechResult$.next(i)},this.recognition.onerror=()=>{this.stopListening()},this.recognition.onend=()=>{this.stopListening()})}startListening(){if(this.recognition&&!this.isListening)try{this.recognition.start()}catch{}}stopListening(){if(this.isListening=!1,this.recognition)try{this.recognition.stop()}catch{}this.avatarState$.next("idle")}speak(e){if(!this.speechEnabled)return;this.currentAudioElement&&(this.currentAudioElement.pause(),this.currentAudioElement=null),"speechSynthesis"in window&&window.speechSynthesis.cancel(),this.pulseInterval&&(clearInterval(this.pulseInterval),this.pulseInterval=null);let t=e.replace(/```[\s\S]*?```/g,"Code block omitted.").replace(/`([^`]+)`/g,"$1").replace(/[*_#]/g,"").replace(/[\u{1F300}-\u{1F9FF}]/gu,"").trim();if(!t)return;let i=`/api/voice/tts?text=${encodeURIComponent(t)}&voice=en-IN-NeerjaNeural`,r=new Audio(i);this.currentAudioElement=r,this.avatarState$.next("speaking"),this.pulseInterval=setInterval(()=>{let s=.3+Math.random()*.7;this.audioIntensity$.next(s)},60),r.onplay=()=>{this.avatarState$.next("speaking")},r.onended=()=>{this.pulseInterval&&clearInterval(this.pulseInterval),this.audioIntensity$.next(0),this.avatarState$.next("idle"),this.currentAudioElement=null},r.onerror=()=>{this.fallbackBrowserSpeech(t)},r.play().catch(()=>{this.fallbackBrowserSpeech(t)})}fallbackBrowserSpeech(e){if(!("speechSynthesis"in window)){this.pulseInterval&&clearInterval(this.pulseInterval),this.audioIntensity$.next(0),this.avatarState$.next("idle");return}let t=new SpeechSynthesisUtterance(e);t.rate=1,t.pitch=1.08;let r=window.speechSynthesis.getVoices().find(s=>s.name.includes("Zira")||s.name.includes("Neerja")||s.name.includes("Aria")||s.name.includes("Jenny")||s.name.includes("Sonia")||s.name.includes("Samantha")||s.name.includes("Female")||s.name.includes("Google UK English Female")||s.name.includes("Google US English Female"));r&&(t.voice=r),t.onend=()=>{this.pulseInterval&&clearInterval(this.pulseInterval),this.audioIntensity$.next(0),this.avatarState$.next("idle")},t.onerror=()=>{this.pulseInterval&&clearInterval(this.pulseInterval),this.audioIntensity$.next(0),this.avatarState$.next("idle")},window.speechSynthesis.speak(t)}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}};var Qt=class n{constructor(e){this.http=e;this.baseUrl=""}getHealth(){return this.http.get(`${this.baseUrl}/health`)}getTelemetry(){return this.http.get(`${this.baseUrl}/api/system/telemetry`)}getSystemState(){return this.http.get(`${this.baseUrl}/api/system/actions/state`)}getProcesses(e=25){return this.http.get(`${this.baseUrl}/api/system/processes?limit=${e}`)}killProcess(e){return this.http.post(`${this.baseUrl}/api/system/processes/kill`,{pid:e})}setVolume(e,t=null){let i={};return e!==null&&(i.level=e),t!==null&&(i.mute=t),this.http.post(`${this.baseUrl}/api/system/actions/volume`,i)}setBrightness(e){return this.http.post(`${this.baseUrl}/api/system/actions/brightness`,{level:e})}launchApp(e,t=null){return this.http.post(`${this.baseUrl}/api/system/actions/app`,{app_name:e,arguments:t})}powerAction(e){return this.http.post(`${this.baseUrl}/api/system/actions/power`,{mode:e})}runMacro(e){return this.http.post(`${this.baseUrl}/api/system/macro`,{macro_name:e})}purgeRam(){return this.http.post(`${this.baseUrl}/api/system/purge-ram`,{})}takeScreenshot(){return this.http.post(`${this.baseUrl}/api/system/automation/screenshot`,{})}executeTerminal(e,t="powershell"){return this.http.post(`${this.baseUrl}/api/system/terminal/execute`,{command:e,shell_type:t})}getStorageStats(){return this.http.get(`${this.baseUrl}/api/storage/stats`)}listStorageFiles(e=""){return this.http.get(`${this.baseUrl}/api/storage/files?subfolder=${encodeURIComponent(e)}`)}uploadStorageFile(e,t="documents"){let i=new FormData;return i.append("file",e),i.append("subfolder",t),this.http.post(`${this.baseUrl}/api/storage/upload`,i)}indexDirectory(e,t=300){return this.http.post(`${this.baseUrl}/api/storage/index-directory`,{directory_path:e,max_files:t})}searchKnowledge(e){return this.http.post(`${this.baseUrl}/api/storage/search`,{query:e})}chatWithAbhi(e,t="abhi",i="main_session"){return this.http.post(`${this.baseUrl}/api/agents/chat`,{message:e,target_agent:t,conversation_id:i})}chatWithJarvis(e,t="abhi",i="main_session"){return this.chatWithAbhi(e,t,i)}dispatchAgent(e,t,i="main_session"){return this.http.post(`${this.baseUrl}/api/agents/dispatch`,{agent_id:e,instruction:t,conversation_id:i})}getAgentStatus(){return this.http.get(`${this.baseUrl}/api/agents/status`)}static{this.\u0275fac=function(t){return new(t||n)(Ze(dc))}}static{this.\u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}};var fc=class n{constructor(e,t){this.audioService=e;this.apiService=t;this.isConnected=!1;this.netSpeedText="NET: ACTIVE";this.screenshotCaptured=new Rt}toggleVoice(){this.audioService.speechEnabled=!this.audioService.speechEnabled,this.audioService.speechEnabled?this.audioService.playSciFiTone("ack"):"speechSynthesis"in window&&window.speechSynthesis.cancel()}takeScreenshot(){this.audioService.playSciFiTone("ack"),this.apiService.takeScreenshot().subscribe(e=>{e.status==="success"&&this.screenshotCaptured.emit(e.preview_base64)})}lockWorkstation(){confirm("Lock host workstation protocols?")&&(this.audioService.playSciFiTone("ack"),this.apiService.powerAction("lock").subscribe())}static{this.\u0275fac=function(t){return new(t||n)(we(Bt),we(Qt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-hud-header"]],inputs:{isConnected:"isConnected",netSpeedText:"netSpeedText"},outputs:{screenshotCaptured:"screenshotCaptured"},decls:33,vars:6,consts:[[1,"hud-header"],[1,"brand"],[1,"brand-icon-box"],[1,"fas","fa-sun","brand-icon"],[1,"brand-title"],[1,"brand-subtitle"],[1,"hud-status-bar"],[1,"status-badge",3,"ngClass"],[1,"indicator"],[1,"status-badge","online"],[1,"fas","fa-book-open",2,"color","var(--neon-gold)"],[1,"fas","fa-network-wired",2,"color","var(--neon-cyan)"],[1,"hud-actions"],[1,"btn-cyber",3,"click"],[1,"fas",3,"ngClass"],[1,"fas","fa-camera"],[1,"btn-cyber","danger",3,"click"],[1,"fas","fa-shield"]],template:function(t,i){t&1&&(L(0,"header",0)(1,"div",1)(2,"div",2),se(3,"i",3),U(),L(4,"div")(5,"div",4),J(6,"ABHI AIOS"),U(),L(7,"div",5),J(8,"CELESTIAL PURANIC MULTI-AGENT OPERATING SYSTEM"),U()()(),L(9,"div",6)(10,"div",7),se(11,"div",8),L(12,"span"),J(13,"LLM: OLLAMA (QWEN3:8B)"),U()(),L(14,"div",9),se(15,"i",10),L(16,"span"),J(17,"SARASWATI VAULT: 50GB"),U()(),L(18,"div",9),se(19,"i",11),L(20,"span"),J(21),U()()(),L(22,"div",12)(23,"button",13),Se("click",function(){return i.toggleVoice()}),se(24,"i",14),L(25,"span"),J(26),U()(),L(27,"button",13),Se("click",function(){return i.takeScreenshot()}),se(28,"i",15),J(29," HANUMAN CAPTURE "),U(),L(30,"button",16),Se("click",function(){return i.lockWorkstation()}),se(31,"i",17),J(32," DURGA SHIELD "),U()()()),t&2&&(Q(10),Oe("ngClass",i.isConnected?"online":"busy"),Q(11),Dt(i.netSpeedText),Q(2),ht("active",i.audioService.speechEnabled),Q(),Oe("ngClass",i.audioService.speechEnabled?"fa-volume-high":"fa-volume-xmark"),Q(2),St("DIVINE SPEECH: ",i.audioService.speechEnabled?"ON":"OFF",""))},dependencies:[Ot,qn],styles:[".hud-header[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;background:var(--bg-panel);border:1px solid var(--border-gold);border-radius:10px;padding:8px 16px;backdrop-filter:blur(14px);box-shadow:0 0 20px #ffd70026;flex-wrap:wrap;gap:8px}.brand[_ngcontent-%COMP%]{display:flex;align-items:center;gap:12px}.brand-icon-box[_ngcontent-%COMP%]{width:38px;height:38px;border-radius:8px;background:#ffd70026;border:1px solid var(--neon-gold);display:flex;align-items:center;justify-content:center;box-shadow:0 0 12px #ffd70066}.brand-icon[_ngcontent-%COMP%]{font-size:20px;color:var(--neon-gold);animation:_ngcontent-%COMP%_pulse-glow 3s infinite ease-in-out}.brand-title[_ngcontent-%COMP%]{font-family:var(--font-celestial);font-size:19px;font-weight:900;letter-spacing:3px;color:var(--neon-gold);text-shadow:0 0 10px rgba(255,215,0,.6)}.brand-subtitle[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:10px;color:var(--neon-cyan);letter-spacing:1.5px}.hud-status-bar[_ngcontent-%COMP%]{display:flex;align-items:center;gap:12px;font-family:var(--font-data);font-size:13px;flex-wrap:wrap}.status-badge[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px;padding:4px 10px;background:#0f1930b3;border:1px solid rgba(255,215,0,.3);border-radius:6px;color:var(--text-main);font-size:12px}.status-badge.online[_ngcontent-%COMP%]   .indicator[_ngcontent-%COMP%]{width:8px;height:8px;border-radius:50%;background:var(--neon-emerald);box-shadow:0 0 8px var(--neon-emerald)}.status-badge.busy[_ngcontent-%COMP%]   .indicator[_ngcontent-%COMP%]{width:8px;height:8px;border-radius:50%;background:var(--neon-gold);box-shadow:0 0 8px var(--neon-gold)}.hud-actions[_ngcontent-%COMP%]{display:flex;gap:8px;align-items:center}.btn-cyber[_ngcontent-%COMP%]{background:#ffd7001f;border:1px solid var(--neon-gold);color:var(--neon-gold);font-family:var(--font-hud);font-size:11px;letter-spacing:1px;padding:6px 12px;border-radius:6px;cursor:pointer;transition:all .2s ease;display:inline-flex;align-items:center;gap:6px}.btn-cyber[_ngcontent-%COMP%]:hover{background:var(--neon-gold);color:#000;box-shadow:0 0 12px var(--neon-gold)}.btn-cyber.active[_ngcontent-%COMP%]{background:var(--neon-gold);color:#000;box-shadow:0 0 10px var(--neon-gold)}.btn-cyber.danger[_ngcontent-%COMP%]{border-color:var(--neon-crimson);color:var(--neon-crimson);background:#ec489926}.btn-cyber.danger[_ngcontent-%COMP%]:hover{background:var(--neon-crimson);color:#fff;box-shadow:0 0 12px var(--neon-crimson)}@keyframes _ngcontent-%COMP%_pulse-glow{0%,to{opacity:.8;filter:drop-shadow(0 0 4px var(--neon-gold))}50%{opacity:1;filter:drop-shadow(0 0 12px var(--neon-gold))}}"]})}};var $T=0,Q_=1,qT=2;var ox=1,XT=2,Li=3,hr=0,bn=1,Ui=2,dr=0,no=1,e0=2,t0=3,n0=4,YT=5,ns=100,ZT=101,JT=102,KT=103,QT=104,eD=200,tD=201,nD=202,iD=203,ep=204,tp=205,rD=206,sD=207,oD=208,aD=209,lD=210,cD=211,uD=212,dD=213,fD=214,np=0,ip=1,rp=2,oo=3,sp=4,op=5,ap=6,lp=7,ax=0,hD=1,pD=2,fr=0,mD=1,gD=2,vD=3,yD=4,_D=5,xD=6,bD=7;var i0=300,ao=301,lo=302,cp=303,up=304,hu=306,dp=1e3,ss=1001,fp=1002,Qn=1003,MD=1004;var hc=1005;var ui=1006,Mh=1007;var os=1008;var Gi=1009,lx=1010,cx=1011,_a=1012,gm=1013,as=1014,Vi=1015,Sa=1016,vm=1017,ym=1018,co=1020,ux=35902,dx=1021,fx=1022,Kn=1023,hx=1024,px=1025,io=1026,uo=1027,mx=1028,_m=1029,gx=1030,xm=1031;var bm=1033,kc=33776,Uc=33777,Vc=33778,Bc=33779,hp=35840,pp=35841,mp=35842,gp=35843,vp=36196,yp=37492,_p=37496,xp=37808,bp=37809,Mp=37810,Ep=37811,Sp=37812,wp=37813,Cp=37814,Tp=37815,Dp=37816,Ap=37817,Ip=37818,Rp=37819,Np=37820,Pp=37821,Hc=36492,Op=36494,Fp=36495,vx=36283,Lp=36284,kp=36285,Up=36286;var zc=2300,Vp=2301,Eh=2302,r0=2400,s0=2401,o0=2402;var ED=3200,SD=3201;var yx=0,wD=1,ur="",Vn="srgb",go="srgb-linear",pu="linear",pt="srgb";var Hs=7680;var a0=519,CD=512,TD=513,DD=514,_x=515,AD=516,ID=517,RD=518,ND=519,l0=35044;var c0="300 es",Bi=2e3,Gc=2001,pr=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){if(this._listeners===void 0)return!1;let i=this._listeners;return i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){if(this._listeners===void 0)return;let r=this._listeners[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){if(this._listeners===void 0)return;let i=this._listeners[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},en=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var Sh=Math.PI/180,Bp=180/Math.PI;function wa(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(en[n&255]+en[n>>8&255]+en[n>>16&255]+en[n>>24&255]+"-"+en[e&255]+en[e>>8&255]+"-"+en[e>>16&15|64]+en[e>>24&255]+"-"+en[t&63|128]+en[t>>8&255]+"-"+en[t>>16&255]+en[t>>24&255]+en[i&255]+en[i>>8&255]+en[i>>16&255]+en[i>>24&255]).toLowerCase()}function xn(n,e,t){return Math.max(e,Math.min(t,n))}function PD(n,e){return(n%e+e)%e}function wh(n,e,t){return(1-t)*n+t*e}function ua(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function _n(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var Qe=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(xn(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Be=class n{constructor(e,t,i,r,s,o,a,l,c){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c)}set(e,t,i,r,s,o,a,l,c){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=l,u[6]=i,u[7]=o,u[8]=c,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],l=i[6],c=i[1],u=i[4],d=i[7],h=i[2],f=i[5],g=i[8],v=r[0],m=r[3],p=r[6],S=r[1],E=r[4],b=r[7],F=r[2],T=r[5],C=r[8];return s[0]=o*v+a*S+l*F,s[3]=o*m+a*E+l*T,s[6]=o*p+a*b+l*C,s[1]=c*v+u*S+d*F,s[4]=c*m+u*E+d*T,s[7]=c*p+u*b+d*C,s[2]=h*v+f*S+g*F,s[5]=h*m+f*E+g*T,s[8]=h*p+f*b+g*C,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8];return t*o*u-t*a*c-i*s*u+i*a*l+r*s*c-r*o*l}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=u*o-a*c,h=a*l-u*s,f=c*s-o*l,g=t*d+i*h+r*f;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let v=1/g;return e[0]=d*v,e[1]=(r*c-u*i)*v,e[2]=(a*i-r*o)*v,e[3]=h*v,e[4]=(u*t-r*l)*v,e[5]=(r*s-a*t)*v,e[6]=f*v,e[7]=(i*l-c*t)*v,e[8]=(o*t-i*s)*v,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let l=Math.cos(s),c=Math.sin(s);return this.set(i*l,i*c,-i*(l*o+c*a)+o+e,-r*c,r*l,-r*(-c*o+l*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Ch.makeScale(e,t)),this}rotate(e){return this.premultiply(Ch.makeRotation(-e)),this}translate(e,t){return this.premultiply(Ch.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Ch=new Be;function xx(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function Wc(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function OD(){let n=Wc("canvas");return n.style.display="block",n}var u0={};function ga(n){n in u0||(u0[n]=!0,console.warn(n))}function FD(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}function LD(n){let e=n.elements;e[2]=.5*e[2]+.5*e[3],e[6]=.5*e[6]+.5*e[7],e[10]=.5*e[10]+.5*e[11],e[14]=.5*e[14]+.5*e[15]}function kD(n){let e=n.elements;e[11]===-1?(e[10]=-e[10]-1,e[14]=-e[14]):(e[10]=-e[10],e[14]=-e[14]+1)}var ot={enabled:!0,workingColorSpace:go,spaces:{},convert:function(n,e,t){return this.enabled===!1||e===t||!e||!t||(this.spaces[e].transfer===pt&&(n.r=zi(n.r),n.g=zi(n.g),n.b=zi(n.b)),this.spaces[e].primaries!==this.spaces[t].primaries&&(n.applyMatrix3(this.spaces[e].toXYZ),n.applyMatrix3(this.spaces[t].fromXYZ)),this.spaces[t].transfer===pt&&(n.r=ro(n.r),n.g=ro(n.g),n.b=ro(n.b))),n},fromWorkingColorSpace:function(n,e){return this.convert(n,this.workingColorSpace,e)},toWorkingColorSpace:function(n,e){return this.convert(n,e,this.workingColorSpace)},getPrimaries:function(n){return this.spaces[n].primaries},getTransfer:function(n){return n===ur?pu:this.spaces[n].transfer},getLuminanceCoefficients:function(n,e=this.workingColorSpace){return n.fromArray(this.spaces[e].luminanceCoefficients)},define:function(n){Object.assign(this.spaces,n)},_getMatrix:function(n,e,t){return n.copy(this.spaces[e].toXYZ).multiply(this.spaces[t].fromXYZ)},_getDrawingBufferColorSpace:function(n){return this.spaces[n].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(n=this.workingColorSpace){return this.spaces[n].workingColorSpaceConfig.unpackColorSpace}};function zi(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function ro(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var d0=[.64,.33,.3,.6,.15,.06],f0=[.2126,.7152,.0722],h0=[.3127,.329],p0=new Be().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),m0=new Be().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);ot.define({[go]:{primaries:d0,whitePoint:h0,transfer:pu,toXYZ:p0,fromXYZ:m0,luminanceCoefficients:f0,workingColorSpaceConfig:{unpackColorSpace:Vn},outputColorSpaceConfig:{drawingBufferColorSpace:Vn}},[Vn]:{primaries:d0,whitePoint:h0,transfer:pt,toXYZ:p0,fromXYZ:m0,luminanceCoefficients:f0,outputColorSpaceConfig:{drawingBufferColorSpace:Vn}}});var zs,Hp=class{static getDataURL(e){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let t;if(e instanceof HTMLCanvasElement)t=e;else{zs===void 0&&(zs=Wc("canvas")),zs.width=e.width,zs.height=e.height;let i=zs.getContext("2d");e instanceof ImageData?i.putImageData(e,0,0):i.drawImage(e,0,0,e.width,e.height),t=zs}return t.width>2048||t.height>2048?(console.warn("THREE.ImageUtils.getDataURL: Image converted to jpg for performance reasons",e),t.toDataURL("image/jpeg",.6)):t.toDataURL("image/png")}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Wc("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=zi(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(zi(t[i]/255)*255):t[i]=zi(t[i]);return{data:t,width:e.width,height:e.height}}else return console.warn("THREE.ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},UD=0,jc=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:UD++}),this.uuid=wa(),this.data=e,this.dataReady=!0,this.version=0}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Th(r[o].image)):s.push(Th(r[o]))}else s=Th(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Th(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Hp.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(console.warn("THREE.Texture: Unable to serialize Texture."),{})}var VD=0,ps=(()=>{class n extends pr{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=ss,s=ss,o=ui,a=os,l=Kn,c=Gi,u=n.DEFAULT_ANISOTROPY,d=ur){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:VD++}),this.uuid=wa(),this.name="",this.source=new jc(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=l,this.internalFormat=null,this.type=c,this.offset=new Qe(0,0),this.repeat=new Qe(1,1),this.center=new Qe(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Be,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.version=0,this.onUpdate=null,this.isRenderTargetTexture=!1,this.pmremVersion=0}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.6,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==i0)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case dp:t.x=t.x-Math.floor(t.x);break;case ss:t.x=t.x<0?0:1;break;case fp:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case dp:t.y=t.y-Math.floor(t.y);break;case ss:t.y=t.y<0?0:1;break;case fp:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=i0,n.DEFAULT_ANISOTROPY=1,n})(),mt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,l=e.elements,c=l[0],u=l[4],d=l[8],h=l[1],f=l[5],g=l[9],v=l[2],m=l[6],p=l[10];if(Math.abs(u-h)<.01&&Math.abs(d-v)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+h)<.1&&Math.abs(d+v)<.1&&Math.abs(g+m)<.1&&Math.abs(c+f+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let E=(c+1)/2,b=(f+1)/2,F=(p+1)/2,T=(u+h)/4,C=(d+v)/4,A=(g+m)/4;return E>b&&E>F?E<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(E),r=T/i,s=C/i):b>F?b<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(b),i=T/r,s=A/r):F<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(F),i=C/s,r=A/s),this.set(i,r,s,t),this}let S=Math.sqrt((m-g)*(m-g)+(d-v)*(d-v)+(h-u)*(h-u));return Math.abs(S)<.001&&(S=1),this.x=(m-g)/S,this.y=(d-v)/S,this.z=(h-u)/S,this.w=Math.acos((c+f+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this.w=Math.max(e.w,Math.min(t.w,this.w)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this.w=Math.max(e,Math.min(t,this.w)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},zp=class extends pr{constructor(e=1,t=1,i={}){super(),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=1,this.scissor=new mt(0,0,e,t),this.scissorTest=!1,this.viewport=new mt(0,0,e,t);let r={width:e,height:t,depth:1};i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:ui,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1},i);let s=new ps(r,i.mapping,i.wrapS,i.wrapT,i.magFilter,i.minFilter,i.format,i.type,i.anisotropy,i.colorSpace);s.flipY=!1,s.generateMipmaps=i.generateMipmaps,s.internalFormat=i.internalFormat,this.textures=[];let o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0;this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.depthTexture=i.depthTexture,this.samples=i.samples}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i;this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let i=0,r=e.textures.length;i<r;i++)this.textures[i]=e.textures[i].clone(),this.textures[i].isRenderTargetTexture=!0;let t=Object.assign({},e.texture.image);return this.texture.source=new jc(t),this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},Wi=class extends zp{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},$c=class extends ps{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Qn,this.minFilter=Qn,this.wrapR=ss,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Gp=class extends ps{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Qn,this.minFilter=Qn,this.wrapR=ss,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var mr=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let l=i[r+0],c=i[r+1],u=i[r+2],d=i[r+3],h=s[o+0],f=s[o+1],g=s[o+2],v=s[o+3];if(a===0){e[t+0]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d;return}if(a===1){e[t+0]=h,e[t+1]=f,e[t+2]=g,e[t+3]=v;return}if(d!==v||l!==h||c!==f||u!==g){let m=1-a,p=l*h+c*f+u*g+d*v,S=p>=0?1:-1,E=1-p*p;if(E>Number.EPSILON){let F=Math.sqrt(E),T=Math.atan2(F,p*S);m=Math.sin(m*T)/F,a=Math.sin(a*T)/F}let b=a*S;if(l=l*m+h*b,c=c*m+f*b,u=u*m+g*b,d=d*m+v*b,m===1-a){let F=1/Math.sqrt(l*l+c*c+u*u+d*d);l*=F,c*=F,u*=F,d*=F}}e[t]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],l=i[r+1],c=i[r+2],u=i[r+3],d=s[o],h=s[o+1],f=s[o+2],g=s[o+3];return e[t]=a*g+u*d+l*f-c*h,e[t+1]=l*g+u*h+c*d-a*f,e[t+2]=c*g+u*f+a*h-l*d,e[t+3]=u*g-a*d-l*h-c*f,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,l=Math.sin,c=a(i/2),u=a(r/2),d=a(s/2),h=l(i/2),f=l(r/2),g=l(s/2);switch(o){case"XYZ":this._x=h*u*d+c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d-h*f*g;break;case"YXZ":this._x=h*u*d+c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d+h*f*g;break;case"ZXY":this._x=h*u*d-c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d-h*f*g;break;case"ZYX":this._x=h*u*d-c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d+h*f*g;break;case"YZX":this._x=h*u*d+c*f*g,this._y=c*f*d+h*u*g,this._z=c*u*g-h*f*d,this._w=c*u*d-h*f*g;break;case"XZY":this._x=h*u*d-c*f*g,this._y=c*f*d-h*u*g,this._z=c*u*g+h*f*d,this._w=c*u*d+h*f*g;break;default:console.warn("THREE.Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],l=t[9],c=t[2],u=t[6],d=t[10],h=i+a+d;if(h>0){let f=.5/Math.sqrt(h+1);this._w=.25/f,this._x=(u-l)*f,this._y=(s-c)*f,this._z=(o-r)*f}else if(i>a&&i>d){let f=2*Math.sqrt(1+i-a-d);this._w=(u-l)/f,this._x=.25*f,this._y=(r+o)/f,this._z=(s+c)/f}else if(a>d){let f=2*Math.sqrt(1+a-i-d);this._w=(s-c)/f,this._x=(r+o)/f,this._y=.25*f,this._z=(l+u)/f}else{let f=2*Math.sqrt(1+d-i-a);this._w=(o-r)/f,this._x=(s+c)/f,this._y=(l+u)/f,this._z=.25*f}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<Number.EPSILON?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(xn(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,l=t._y,c=t._z,u=t._w;return this._x=i*u+o*a+r*c-s*l,this._y=r*u+o*l+s*a-i*c,this._z=s*u+o*c+i*l-r*a,this._w=o*u-i*a-r*l-s*c,this._onChangeCallback(),this}slerp(e,t){if(t===0)return this;if(t===1)return this.copy(e);let i=this._x,r=this._y,s=this._z,o=this._w,a=o*e._w+i*e._x+r*e._y+s*e._z;if(a<0?(this._w=-e._w,this._x=-e._x,this._y=-e._y,this._z=-e._z,a=-a):this.copy(e),a>=1)return this._w=o,this._x=i,this._y=r,this._z=s,this;let l=1-a*a;if(l<=Number.EPSILON){let f=1-t;return this._w=f*o+t*this._w,this._x=f*i+t*this._x,this._y=f*r+t*this._y,this._z=f*s+t*this._z,this.normalize(),this}let c=Math.sqrt(l),u=Math.atan2(c,a),d=Math.sin((1-t)*u)/c,h=Math.sin(t*u)/c;return this._w=o*d+this._w*h,this._x=i*d+this._x*h,this._y=r*d+this._y*h,this._z=s*d+this._z*h,this._onChangeCallback(),this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},O=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(g0.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(g0.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,l=e.w,c=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+l*c+o*d-a*u,this.y=i+l*u+a*c-s*d,this.z=r+l*d+s*u-o*c,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,l=t.z;return this.x=r*l-s*a,this.y=s*o-i*l,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return Dh.copy(this).projectOnVector(e),this.sub(Dh)}reflect(e){return this.sub(Dh.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(xn(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},Dh=new O,g0=new mr,ls=class{constructor(e=new O(1/0,1/0,1/0),t=new O(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Yn.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Yn.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Yn.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Yn):Yn.fromBufferAttribute(s,o),Yn.applyMatrix4(e.matrixWorld),this.expandByPoint(Yn);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),pc.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),pc.copy(i.boundingBox)),pc.applyMatrix4(e.matrixWorld),this.union(pc)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Yn),Yn.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(da),mc.subVectors(this.max,da),Gs.subVectors(e.a,da),Ws.subVectors(e.b,da),js.subVectors(e.c,da),rr.subVectors(Ws,Gs),sr.subVectors(js,Ws),Yr.subVectors(Gs,js);let t=[0,-rr.z,rr.y,0,-sr.z,sr.y,0,-Yr.z,Yr.y,rr.z,0,-rr.x,sr.z,0,-sr.x,Yr.z,0,-Yr.x,-rr.y,rr.x,0,-sr.y,sr.x,0,-Yr.y,Yr.x,0];return!Ah(t,Gs,Ws,js,mc)||(t=[1,0,0,0,1,0,0,0,1],!Ah(t,Gs,Ws,js,mc))?!1:(gc.crossVectors(rr,sr),t=[gc.x,gc.y,gc.z],Ah(t,Gs,Ws,js,mc))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Yn).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Yn).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Ri[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Ri[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Ri[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Ri[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Ri[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Ri[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Ri[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Ri[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Ri),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}},Ri=[new O,new O,new O,new O,new O,new O,new O,new O],Yn=new O,pc=new ls,Gs=new O,Ws=new O,js=new O,rr=new O,sr=new O,Yr=new O,da=new O,mc=new O,gc=new O,Zr=new O;function Ah(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){Zr.fromArray(n,s);let a=r.x*Math.abs(Zr.x)+r.y*Math.abs(Zr.y)+r.z*Math.abs(Zr.z),l=e.dot(Zr),c=t.dot(Zr),u=i.dot(Zr);if(Math.max(-Math.max(l,c,u),Math.min(l,c,u))>a)return!1}return!0}var BD=new ls,fa=new O,Ih=new O,fo=class{constructor(e=new O,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):BD.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;fa.subVectors(e,this.center);let t=fa.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(fa,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Ih.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(fa.copy(e.center).add(Ih)),this.expandByPoint(fa.copy(e.center).sub(Ih))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}},Ni=new O,Rh=new O,vc=new O,or=new O,Nh=new O,yc=new O,Ph=new O,qc=class{constructor(e=new O,t=new O(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Ni)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Ni.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Ni.copy(this.origin).addScaledVector(this.direction,t),Ni.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Rh.copy(e).add(t).multiplyScalar(.5),vc.copy(t).sub(e).normalize(),or.copy(this.origin).sub(Rh);let s=e.distanceTo(t)*.5,o=-this.direction.dot(vc),a=or.dot(this.direction),l=-or.dot(vc),c=or.lengthSq(),u=Math.abs(1-o*o),d,h,f,g;if(u>0)if(d=o*l-a,h=o*a-l,g=s*u,d>=0)if(h>=-g)if(h<=g){let v=1/u;d*=v,h*=v,f=d*(d+o*h+2*a)+h*(o*d+h+2*l)+c}else h=s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h=-s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h<=-g?(d=Math.max(0,-(-o*s+a)),h=d>0?-s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c):h<=g?(d=0,h=Math.min(Math.max(-s,-l),s),f=h*(h+2*l)+c):(d=Math.max(0,-(o*s+a)),h=d>0?s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c);else h=o>0?-s:s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Rh).addScaledVector(vc,h),f}intersectSphere(e,t){Ni.subVectors(e.center,this.origin);let i=Ni.dot(this.direction),r=Ni.dot(Ni)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,l=i+o;return l<0?null:a<0?this.at(l,t):this.at(a,t)}intersectsSphere(e){return this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,l,c=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,h=this.origin;return c>=0?(i=(e.min.x-h.x)*c,r=(e.max.x-h.x)*c):(i=(e.max.x-h.x)*c,r=(e.min.x-h.x)*c),u>=0?(s=(e.min.y-h.y)*u,o=(e.max.y-h.y)*u):(s=(e.max.y-h.y)*u,o=(e.min.y-h.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-h.z)*d,l=(e.max.z-h.z)*d):(a=(e.max.z-h.z)*d,l=(e.min.z-h.z)*d),i>l||a>r)||((a>i||i!==i)&&(i=a),(l<r||r!==r)&&(r=l),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Ni)!==null}intersectTriangle(e,t,i,r,s){Nh.subVectors(t,e),yc.subVectors(i,e),Ph.crossVectors(Nh,yc);let o=this.direction.dot(Ph),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;or.subVectors(this.origin,e);let l=a*this.direction.dot(yc.crossVectors(or,yc));if(l<0)return null;let c=a*this.direction.dot(Nh.cross(or));if(c<0||l+c>o)return null;let u=-a*or.dot(Ph);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},At=class n{constructor(e,t,i,r,s,o,a,l,c,u,d,h,f,g,v,m){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c,u,d,h,f,g,v,m)}set(e,t,i,r,s,o,a,l,c,u,d,h,f,g,v,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=l,p[2]=c,p[6]=u,p[10]=d,p[14]=h,p[3]=f,p[7]=g,p[11]=v,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){let t=this.elements,i=e.elements,r=1/$s.setFromMatrixColumn(e,0).length(),s=1/$s.setFromMatrixColumn(e,1).length(),o=1/$s.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),l=Math.cos(r),c=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let h=o*u,f=o*d,g=a*u,v=a*d;t[0]=l*u,t[4]=-l*d,t[8]=c,t[1]=f+g*c,t[5]=h-v*c,t[9]=-a*l,t[2]=v-h*c,t[6]=g+f*c,t[10]=o*l}else if(e.order==="YXZ"){let h=l*u,f=l*d,g=c*u,v=c*d;t[0]=h+v*a,t[4]=g*a-f,t[8]=o*c,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=f*a-g,t[6]=v+h*a,t[10]=o*l}else if(e.order==="ZXY"){let h=l*u,f=l*d,g=c*u,v=c*d;t[0]=h-v*a,t[4]=-o*d,t[8]=g+f*a,t[1]=f+g*a,t[5]=o*u,t[9]=v-h*a,t[2]=-o*c,t[6]=a,t[10]=o*l}else if(e.order==="ZYX"){let h=o*u,f=o*d,g=a*u,v=a*d;t[0]=l*u,t[4]=g*c-f,t[8]=h*c+v,t[1]=l*d,t[5]=v*c+h,t[9]=f*c-g,t[2]=-c,t[6]=a*l,t[10]=o*l}else if(e.order==="YZX"){let h=o*l,f=o*c,g=a*l,v=a*c;t[0]=l*u,t[4]=v-h*d,t[8]=g*d+f,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-c*u,t[6]=f*d+g,t[10]=h-v*d}else if(e.order==="XZY"){let h=o*l,f=o*c,g=a*l,v=a*c;t[0]=l*u,t[4]=-d,t[8]=c*u,t[1]=h*d+v,t[5]=o*u,t[9]=f*d-g,t[2]=g*d-f,t[6]=a*u,t[10]=v*d+h}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(HD,e,zD)}lookAt(e,t,i){let r=this.elements;return Sn.subVectors(e,t),Sn.lengthSq()===0&&(Sn.z=1),Sn.normalize(),ar.crossVectors(i,Sn),ar.lengthSq()===0&&(Math.abs(i.z)===1?Sn.x+=1e-4:Sn.z+=1e-4,Sn.normalize(),ar.crossVectors(i,Sn)),ar.normalize(),_c.crossVectors(Sn,ar),r[0]=ar.x,r[4]=_c.x,r[8]=Sn.x,r[1]=ar.y,r[5]=_c.y,r[9]=Sn.y,r[2]=ar.z,r[6]=_c.z,r[10]=Sn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],l=i[8],c=i[12],u=i[1],d=i[5],h=i[9],f=i[13],g=i[2],v=i[6],m=i[10],p=i[14],S=i[3],E=i[7],b=i[11],F=i[15],T=r[0],C=r[4],A=r[8],M=r[12],_=r[1],D=r[5],z=r[9],H=r[13],$=r[2],K=r[6],j=r[10],te=r[14],W=r[3],le=r[7],pe=r[11],Ce=r[15];return s[0]=o*T+a*_+l*$+c*W,s[4]=o*C+a*D+l*K+c*le,s[8]=o*A+a*z+l*j+c*pe,s[12]=o*M+a*H+l*te+c*Ce,s[1]=u*T+d*_+h*$+f*W,s[5]=u*C+d*D+h*K+f*le,s[9]=u*A+d*z+h*j+f*pe,s[13]=u*M+d*H+h*te+f*Ce,s[2]=g*T+v*_+m*$+p*W,s[6]=g*C+v*D+m*K+p*le,s[10]=g*A+v*z+m*j+p*pe,s[14]=g*M+v*H+m*te+p*Ce,s[3]=S*T+E*_+b*$+F*W,s[7]=S*C+E*D+b*K+F*le,s[11]=S*A+E*z+b*j+F*pe,s[15]=S*M+E*H+b*te+F*Ce,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],l=e[9],c=e[13],u=e[2],d=e[6],h=e[10],f=e[14],g=e[3],v=e[7],m=e[11],p=e[15];return g*(+s*l*d-r*c*d-s*a*h+i*c*h+r*a*f-i*l*f)+v*(+t*l*f-t*c*h+s*o*h-r*o*f+r*c*u-s*l*u)+m*(+t*c*d-t*a*f-s*o*d+i*o*f+s*a*u-i*c*u)+p*(-r*a*u-t*l*d+t*a*h+r*o*d-i*o*h+i*l*u)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=e[9],h=e[10],f=e[11],g=e[12],v=e[13],m=e[14],p=e[15],S=d*m*c-v*h*c+v*l*f-a*m*f-d*l*p+a*h*p,E=g*h*c-u*m*c-g*l*f+o*m*f+u*l*p-o*h*p,b=u*v*c-g*d*c+g*a*f-o*v*f-u*a*p+o*d*p,F=g*d*l-u*v*l-g*a*h+o*v*h+u*a*m-o*d*m,T=t*S+i*E+r*b+s*F;if(T===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let C=1/T;return e[0]=S*C,e[1]=(v*h*s-d*m*s-v*r*f+i*m*f+d*r*p-i*h*p)*C,e[2]=(a*m*s-v*l*s+v*r*c-i*m*c-a*r*p+i*l*p)*C,e[3]=(d*l*s-a*h*s-d*r*c+i*h*c+a*r*f-i*l*f)*C,e[4]=E*C,e[5]=(u*m*s-g*h*s+g*r*f-t*m*f-u*r*p+t*h*p)*C,e[6]=(g*l*s-o*m*s-g*r*c+t*m*c+o*r*p-t*l*p)*C,e[7]=(o*h*s-u*l*s+u*r*c-t*h*c-o*r*f+t*l*f)*C,e[8]=b*C,e[9]=(g*d*s-u*v*s-g*i*f+t*v*f+u*i*p-t*d*p)*C,e[10]=(o*v*s-g*a*s+g*i*c-t*v*c-o*i*p+t*a*p)*C,e[11]=(u*a*s-o*d*s-u*i*c+t*d*c+o*i*f-t*a*f)*C,e[12]=F*C,e[13]=(u*v*r-g*d*r+g*i*h-t*v*h-u*i*m+t*d*m)*C,e[14]=(g*a*r-o*v*r-g*i*l+t*v*l+o*i*m-t*a*m)*C,e[15]=(o*d*r-u*a*r+u*i*l-t*d*l-o*i*h+t*a*h)*C,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,l=e.z,c=s*o,u=s*a;return this.set(c*o+i,c*a-r*l,c*l+r*a,0,c*a+r*l,u*a+i,u*l-r*o,0,c*l-r*a,u*l+r*o,s*l*l+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,l=t._w,c=s+s,u=o+o,d=a+a,h=s*c,f=s*u,g=s*d,v=o*u,m=o*d,p=a*d,S=l*c,E=l*u,b=l*d,F=i.x,T=i.y,C=i.z;return r[0]=(1-(v+p))*F,r[1]=(f+b)*F,r[2]=(g-E)*F,r[3]=0,r[4]=(f-b)*T,r[5]=(1-(h+p))*T,r[6]=(m+S)*T,r[7]=0,r[8]=(g+E)*C,r[9]=(m-S)*C,r[10]=(1-(h+v))*C,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements,s=$s.set(r[0],r[1],r[2]).length(),o=$s.set(r[4],r[5],r[6]).length(),a=$s.set(r[8],r[9],r[10]).length();this.determinant()<0&&(s=-s),e.x=r[12],e.y=r[13],e.z=r[14],Zn.copy(this);let c=1/s,u=1/o,d=1/a;return Zn.elements[0]*=c,Zn.elements[1]*=c,Zn.elements[2]*=c,Zn.elements[4]*=u,Zn.elements[5]*=u,Zn.elements[6]*=u,Zn.elements[8]*=d,Zn.elements[9]*=d,Zn.elements[10]*=d,t.setFromRotationMatrix(Zn),i.x=s,i.y=o,i.z=a,this}makePerspective(e,t,i,r,s,o,a=Bi){let l=this.elements,c=2*s/(t-e),u=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),f,g;if(a===Bi)f=-(o+s)/(o-s),g=-2*o*s/(o-s);else if(a===Gc)f=-o/(o-s),g=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=c,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=u,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=f,l[14]=g,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=Bi){let l=this.elements,c=1/(t-e),u=1/(i-r),d=1/(o-s),h=(t+e)*c,f=(i+r)*u,g,v;if(a===Bi)g=(o+s)*d,v=-2*d;else if(a===Gc)g=s*d,v=-1*d;else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=2*c,l[4]=0,l[8]=0,l[12]=-h,l[1]=0,l[5]=2*u,l[9]=0,l[13]=-f,l[2]=0,l[6]=0,l[10]=v,l[14]=-g,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},$s=new O,Zn=new At,HD=new O(0,0,0),zD=new O(1,1,1),ar=new O,_c=new O,Sn=new O,v0=new At,y0=new mr,cs=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],l=s[8],c=s[1],u=s[5],d=s[9],h=s[2],f=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(xn(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(f,u),this._z=0);break;case"YXZ":this._x=Math.asin(-xn(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(l,g),this._z=Math.atan2(c,u)):(this._y=Math.atan2(-h,o),this._z=0);break;case"ZXY":this._x=Math.asin(xn(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(-h,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(c,o));break;case"ZYX":this._y=Math.asin(-xn(h,-1,1)),Math.abs(h)<.9999999?(this._x=Math.atan2(f,g),this._z=Math.atan2(c,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(xn(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-h,o)):(this._x=0,this._y=Math.atan2(l,g));break;case"XZY":this._z=Math.asin(-xn(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(f,u),this._y=Math.atan2(l,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:console.warn("THREE.Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return v0.makeRotationFromQuaternion(t),this.setFromRotationMatrix(v0,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return y0.setFromEuler(this),this.setFromQuaternion(y0,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Xc=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},GD=0,_0=new O,qs=new mr,Pi=new At,xc=new O,ha=new O,WD=new O,jD=new mr,x0=new O(1,0,0),b0=new O(0,1,0),M0=new O(0,0,1),E0={type:"added"},$D={type:"removed"},Xs={type:"childadded",child:null},Oh={type:"childremoved",child:null},vr=(()=>{class n extends pr{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:GD++}),this.uuid=wa(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new O,i=new cs,r=new mr,s=new O(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new At},normalMatrix:{value:new Be}}),this.matrix=new At,this.matrixWorld=new At,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Xc,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.userData={}}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return qs.setFromAxisAngle(t,i),this.quaternion.multiply(qs),this}rotateOnWorldAxis(t,i){return qs.setFromAxisAngle(t,i),this.quaternion.premultiply(qs),this}rotateX(t){return this.rotateOnAxis(x0,t)}rotateY(t){return this.rotateOnAxis(b0,t)}rotateZ(t){return this.rotateOnAxis(M0,t)}translateOnAxis(t,i){return _0.copy(t).applyQuaternion(this.quaternion),this.position.add(_0.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(x0,t)}translateY(t){return this.translateOnAxis(b0,t)}translateZ(t){return this.translateOnAxis(M0,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Pi.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?xc.copy(t):xc.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),ha.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Pi.lookAt(ha,xc,this.up):Pi.lookAt(xc,ha,this.up),this.quaternion.setFromRotationMatrix(Pi),s&&(Pi.extractRotation(s.matrixWorld),qs.setFromRotationMatrix(Pi),this.quaternion.premultiply(qs.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(console.error("THREE.Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(E0),Xs.child=t,this.dispatchEvent(Xs),Xs.child=null):console.error("THREE.Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent($D),Oh.child=t,this.dispatchEvent(Oh),Oh.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Pi.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Pi.multiply(t.parent.matrixWorld)),t.applyMatrix4(Pi),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(E0),Xs.child=t,this.dispatchEvent(Xs),Xs.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(ha,t,WD),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(ha,jD,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale),this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.6,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.visibility=this._visibility,s.active=this._active,s.bounds=this._bounds.map(l=>({boxInitialized:l.boxInitialized,boxMin:l.box.min.toArray(),boxMax:l.box.max.toArray(),sphereInitialized:l.sphereInitialized,sphereRadius:l.sphere.radius,sphereCenter:l.sphere.center.toArray()})),s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.geometryCount=this._geometryCount,s.matricesTexture=this._matricesTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere={center:s.boundingSphere.center.toArray(),radius:s.boundingSphere.radius}),this.boundingBox!==null&&(s.boundingBox={min:s.boundingBox.min.toArray(),max:s.boundingBox.max.toArray()}));function o(l,c){return l[c.uuid]===void 0&&(l[c.uuid]=c.toJSON(t)),c.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let l=this.geometry.parameters;if(l!==void 0&&l.shapes!==void 0){let c=l.shapes;if(Array.isArray(c))for(let u=0,d=c.length;u<d;u++){let h=c[u];o(t.shapes,h)}else o(t.shapes,c)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let l=[];for(let c=0,u=this.material.length;c<u;c++)l.push(o(t.materials,this.material[c]));s.material=l}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let l=0;l<this.children.length;l++)s.children.push(this.children[l].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let l=0;l<this.animations.length;l++){let c=this.animations[l];s.animations.push(o(t.animations,c))}}if(i){let l=a(t.geometries),c=a(t.materials),u=a(t.textures),d=a(t.images),h=a(t.shapes),f=a(t.skeletons),g=a(t.animations),v=a(t.nodes);l.length>0&&(r.geometries=l),c.length>0&&(r.materials=c),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),h.length>0&&(r.shapes=h),f.length>0&&(r.skeletons=f),g.length>0&&(r.animations=g),v.length>0&&(r.nodes=v)}return r.object=s,r;function a(l){let c=[];for(let u in l){let d=l[u];delete d.metadata,c.push(d)}return c}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new O(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Jn=new O,Oi=new O,Fh=new O,Fi=new O,Ys=new O,Zs=new O,S0=new O,Lh=new O,kh=new O,Uh=new O,Vh=new mt,Bh=new mt,Hh=new mt,is=class n{constructor(e=new O,t=new O,i=new O){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Jn.subVectors(e,t),r.cross(Jn);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Jn.subVectors(r,t),Oi.subVectors(i,t),Fh.subVectors(e,t);let o=Jn.dot(Jn),a=Jn.dot(Oi),l=Jn.dot(Fh),c=Oi.dot(Oi),u=Oi.dot(Fh),d=o*c-a*a;if(d===0)return s.set(0,0,0),null;let h=1/d,f=(c*l-a*u)*h,g=(o*u-a*l)*h;return s.set(1-f-g,g,f)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Fi)===null?!1:Fi.x>=0&&Fi.y>=0&&Fi.x+Fi.y<=1}static getInterpolation(e,t,i,r,s,o,a,l){return this.getBarycoord(e,t,i,r,Fi)===null?(l.x=0,l.y=0,"z"in l&&(l.z=0),"w"in l&&(l.w=0),null):(l.setScalar(0),l.addScaledVector(s,Fi.x),l.addScaledVector(o,Fi.y),l.addScaledVector(a,Fi.z),l)}static getInterpolatedAttribute(e,t,i,r,s,o){return Vh.setScalar(0),Bh.setScalar(0),Hh.setScalar(0),Vh.fromBufferAttribute(e,t),Bh.fromBufferAttribute(e,i),Hh.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Vh,s.x),o.addScaledVector(Bh,s.y),o.addScaledVector(Hh,s.z),o}static isFrontFacing(e,t,i,r){return Jn.subVectors(i,t),Oi.subVectors(e,t),Jn.cross(Oi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Jn.subVectors(this.c,this.b),Oi.subVectors(this.a,this.b),Jn.cross(Oi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;Ys.subVectors(r,i),Zs.subVectors(s,i),Lh.subVectors(e,i);let l=Ys.dot(Lh),c=Zs.dot(Lh);if(l<=0&&c<=0)return t.copy(i);kh.subVectors(e,r);let u=Ys.dot(kh),d=Zs.dot(kh);if(u>=0&&d<=u)return t.copy(r);let h=l*d-u*c;if(h<=0&&l>=0&&u<=0)return o=l/(l-u),t.copy(i).addScaledVector(Ys,o);Uh.subVectors(e,s);let f=Ys.dot(Uh),g=Zs.dot(Uh);if(g>=0&&f<=g)return t.copy(s);let v=f*c-l*g;if(v<=0&&c>=0&&g<=0)return a=c/(c-g),t.copy(i).addScaledVector(Zs,a);let m=u*g-f*d;if(m<=0&&d-u>=0&&f-g>=0)return S0.subVectors(s,r),a=(d-u)/(d-u+(f-g)),t.copy(r).addScaledVector(S0,a);let p=1/(m+v+h);return o=v*p,a=h*p,t.copy(i).addScaledVector(Ys,o).addScaledVector(Zs,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},bx={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},lr={h:0,s:0,l:0},bc={h:0,s:0,l:0};function zh(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var $e=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Vn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,ot.toWorkingColorSpace(this,t),this}setRGB(e,t,i,r=ot.workingColorSpace){return this.r=e,this.g=t,this.b=i,ot.toWorkingColorSpace(this,r),this}setHSL(e,t,i,r=ot.workingColorSpace){if(e=PD(e,1),t=xn(t,0,1),i=xn(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=zh(o,s,e+1/3),this.g=zh(o,s,e),this.b=zh(o,s,e-1/3)}return ot.toWorkingColorSpace(this,r),this}setStyle(e,t=Vn){function i(s){s!==void 0&&parseFloat(s)<1&&console.warn("THREE.Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:console.warn("THREE.Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);console.warn("THREE.Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Vn){let i=bx[e.toLowerCase()];return i!==void 0?this.setHex(i,t):console.warn("THREE.Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=zi(e.r),this.g=zi(e.g),this.b=zi(e.b),this}copyLinearToSRGB(e){return this.r=ro(e.r),this.g=ro(e.g),this.b=ro(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Vn){return ot.fromWorkingColorSpace(tn.copy(this),e),Math.round(xn(tn.r*255,0,255))*65536+Math.round(xn(tn.g*255,0,255))*256+Math.round(xn(tn.b*255,0,255))}getHexString(e=Vn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=ot.workingColorSpace){ot.fromWorkingColorSpace(tn.copy(this),t);let i=tn.r,r=tn.g,s=tn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),l,c,u=(a+o)/2;if(a===o)l=0,c=0;else{let d=o-a;switch(c=u<=.5?d/(o+a):d/(2-o-a),o){case i:l=(r-s)/d+(r<s?6:0);break;case r:l=(s-i)/d+2;break;case s:l=(i-r)/d+4;break}l/=6}return e.h=l,e.s=c,e.l=u,e}getRGB(e,t=ot.workingColorSpace){return ot.fromWorkingColorSpace(tn.copy(this),t),e.r=tn.r,e.g=tn.g,e.b=tn.b,e}getStyle(e=Vn){ot.fromWorkingColorSpace(tn.copy(this),e);let t=tn.r,i=tn.g,r=tn.b;return e!==Vn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(lr),this.setHSL(lr.h+e,lr.s+t,lr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(lr),e.getHSL(bc);let i=wh(lr.h,bc.h,t),r=wh(lr.s,bc.s,t),s=wh(lr.l,bc.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},tn=new $e;$e.NAMES=bx;var qD=0,gr=class extends pr{static get type(){return"Material"}get type(){return this.constructor.type}set type(e){}constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:qD++}),this.uuid=wa(),this.name="",this.blending=no,this.side=hr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=ep,this.blendDst=tp,this.blendEquation=ns,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new $e(0,0,0),this.blendAlpha=0,this.depthFunc=oo,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=a0,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Hs,this.stencilZFail=Hs,this.stencilZPass=Hs,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){console.warn(`THREE.Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){console.warn(`THREE.Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.6,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==no&&(i.blending=this.blending),this.side!==hr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==ep&&(i.blendSrc=this.blendSrc),this.blendDst!==tp&&(i.blendDst=this.blendDst),this.blendEquation!==ns&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==oo&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==a0&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Hs&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Hs&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Hs&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let l=s[a];delete l.metadata,o.push(l)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}onBuild(){console.warn("Material: onBuild() has been removed.")}},us=class extends gr{static get type(){return"MeshBasicMaterial"}constructor(e){super(),this.isMeshBasicMaterial=!0,this.color=new $e(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new cs,this.combine=ax,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};var Ht=new O,Mc=new Qe,ln=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=l0,this.updateRanges=[],this.gpuType=Vi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Mc.fromBufferAttribute(this,t),Mc.applyMatrix3(e),this.setXY(t,Mc.x,Mc.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Ht.fromBufferAttribute(this,t),Ht.applyMatrix3(e),this.setXYZ(t,Ht.x,Ht.y,Ht.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Ht.fromBufferAttribute(this,t),Ht.applyMatrix4(e),this.setXYZ(t,Ht.x,Ht.y,Ht.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Ht.fromBufferAttribute(this,t),Ht.applyNormalMatrix(e),this.setXYZ(t,Ht.x,Ht.y,Ht.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Ht.fromBufferAttribute(this,t),Ht.transformDirection(e),this.setXYZ(t,Ht.x,Ht.y,Ht.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=ua(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=_n(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=ua(t,this.array)),t}setX(e,t){return this.normalized&&(t=_n(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=ua(t,this.array)),t}setY(e,t){return this.normalized&&(t=_n(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=ua(t,this.array)),t}setZ(e,t){return this.normalized&&(t=_n(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=ua(t,this.array)),t}setW(e,t){return this.normalized&&(t=_n(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=_n(t,this.array),i=_n(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=_n(t,this.array),i=_n(i,this.array),r=_n(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=_n(t,this.array),i=_n(i,this.array),r=_n(r,this.array),s=_n(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==l0&&(e.usage=this.usage),e}};var Yc=class extends ln{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Zc=class extends ln{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Yt=class extends ln{constructor(e,t,i){super(new Float32Array(e),t,i)}},XD=0,Un=new At,Gh=new vr,Js=new O,wn=new ls,pa=new ls,qt=new O,Cn=class n extends pr{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:XD++}),this.uuid=wa(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(xx(e)?Zc:Yc)(e,1):this.index=e,this}setIndirect(e){return this.indirect=e,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Be().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Un.makeRotationFromQuaternion(e),this.applyMatrix4(Un),this}rotateX(e){return Un.makeRotationX(e),this.applyMatrix4(Un),this}rotateY(e){return Un.makeRotationY(e),this.applyMatrix4(Un),this}rotateZ(e){return Un.makeRotationZ(e),this.applyMatrix4(Un),this}translate(e,t,i){return Un.makeTranslation(e,t,i),this.applyMatrix4(Un),this}scale(e,t,i){return Un.makeScale(e,t,i),this.applyMatrix4(Un),this}lookAt(e){return Gh.lookAt(e),Gh.updateMatrix(),this.applyMatrix4(Gh.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Js).negate(),this.translate(Js.x,Js.y,Js.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new Yt(i,3))}else{for(let i=0,r=t.count;i<r;i++){let s=e[i];t.setXYZ(i,s.x,s.y,s.z||0)}e.length>t.count&&console.warn("THREE.BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new ls);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new O(-1/0,-1/0,-1/0),new O(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];wn.setFromBufferAttribute(s),this.morphTargetsRelative?(qt.addVectors(this.boundingBox.min,wn.min),this.boundingBox.expandByPoint(qt),qt.addVectors(this.boundingBox.max,wn.max),this.boundingBox.expandByPoint(qt)):(this.boundingBox.expandByPoint(wn.min),this.boundingBox.expandByPoint(wn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&console.error('THREE.BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new fo);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new O,1/0);return}if(e){let i=this.boundingSphere.center;if(wn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];pa.setFromBufferAttribute(a),this.morphTargetsRelative?(qt.addVectors(wn.min,pa.min),wn.expandByPoint(qt),qt.addVectors(wn.max,pa.max),wn.expandByPoint(qt)):(wn.expandByPoint(pa.min),wn.expandByPoint(pa.max))}wn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)qt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(qt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],l=this.morphTargetsRelative;for(let c=0,u=a.count;c<u;c++)qt.fromBufferAttribute(a,c),l&&(Js.fromBufferAttribute(e,c),qt.add(Js)),r=Math.max(r,i.distanceToSquared(qt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error('THREE.BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){console.error("THREE.BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new ln(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],l=[];for(let A=0;A<i.count;A++)a[A]=new O,l[A]=new O;let c=new O,u=new O,d=new O,h=new Qe,f=new Qe,g=new Qe,v=new O,m=new O;function p(A,M,_){c.fromBufferAttribute(i,A),u.fromBufferAttribute(i,M),d.fromBufferAttribute(i,_),h.fromBufferAttribute(s,A),f.fromBufferAttribute(s,M),g.fromBufferAttribute(s,_),u.sub(c),d.sub(c),f.sub(h),g.sub(h);let D=1/(f.x*g.y-g.x*f.y);isFinite(D)&&(v.copy(u).multiplyScalar(g.y).addScaledVector(d,-f.y).multiplyScalar(D),m.copy(d).multiplyScalar(f.x).addScaledVector(u,-g.x).multiplyScalar(D),a[A].add(v),a[M].add(v),a[_].add(v),l[A].add(m),l[M].add(m),l[_].add(m))}let S=this.groups;S.length===0&&(S=[{start:0,count:e.count}]);for(let A=0,M=S.length;A<M;++A){let _=S[A],D=_.start,z=_.count;for(let H=D,$=D+z;H<$;H+=3)p(e.getX(H+0),e.getX(H+1),e.getX(H+2))}let E=new O,b=new O,F=new O,T=new O;function C(A){F.fromBufferAttribute(r,A),T.copy(F);let M=a[A];E.copy(M),E.sub(F.multiplyScalar(F.dot(M))).normalize(),b.crossVectors(T,M);let D=b.dot(l[A])<0?-1:1;o.setXYZW(A,E.x,E.y,E.z,D)}for(let A=0,M=S.length;A<M;++A){let _=S[A],D=_.start,z=_.count;for(let H=D,$=D+z;H<$;H+=3)C(e.getX(H+0)),C(e.getX(H+1)),C(e.getX(H+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new ln(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let h=0,f=i.count;h<f;h++)i.setXYZ(h,0,0,0);let r=new O,s=new O,o=new O,a=new O,l=new O,c=new O,u=new O,d=new O;if(e)for(let h=0,f=e.count;h<f;h+=3){let g=e.getX(h+0),v=e.getX(h+1),m=e.getX(h+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,v),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),l.fromBufferAttribute(i,v),c.fromBufferAttribute(i,m),a.add(u),l.add(u),c.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(v,l.x,l.y,l.z),i.setXYZ(m,c.x,c.y,c.z)}else for(let h=0,f=t.count;h<f;h+=3)r.fromBufferAttribute(t,h+0),s.fromBufferAttribute(t,h+1),o.fromBufferAttribute(t,h+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(h+0,u.x,u.y,u.z),i.setXYZ(h+1,u.x,u.y,u.z),i.setXYZ(h+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)qt.fromBufferAttribute(e,t),qt.normalize(),e.setXYZ(t,qt.x,qt.y,qt.z)}toNonIndexed(){function e(a,l){let c=a.array,u=a.itemSize,d=a.normalized,h=new c.constructor(l.length*u),f=0,g=0;for(let v=0,m=l.length;v<m;v++){a.isInterleavedBufferAttribute?f=l[v]*a.data.stride+a.offset:f=l[v]*u;for(let p=0;p<u;p++)h[g++]=c[f++]}return new ln(h,u,d)}if(this.index===null)return console.warn("THREE.BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let l=r[a],c=e(l,i);t.setAttribute(a,c)}let s=this.morphAttributes;for(let a in s){let l=[],c=s[a];for(let u=0,d=c.length;u<d;u++){let h=c[u],f=e(h,i);l.push(f)}t.morphAttributes[a]=l}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,l=o.length;a<l;a++){let c=o[a];t.addGroup(c.start,c.count,c.materialIndex)}return t}toJSON(){let e={metadata:{version:4.6,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let l=this.parameters;for(let c in l)l[c]!==void 0&&(e[c]=l[c]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let l in i){let c=i[l];e.data.attributes[l]=c.toJSON(e.data)}let r={},s=!1;for(let l in this.morphAttributes){let c=this.morphAttributes[l],u=[];for(let d=0,h=c.length;d<h;d++){let f=c[d];u.push(f.toJSON(e.data))}u.length>0&&(r[l]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere={center:a.center.toArray(),radius:a.radius}),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone(t));let r=e.attributes;for(let c in r){let u=r[c];this.setAttribute(c,u.clone(t))}let s=e.morphAttributes;for(let c in s){let u=[],d=s[c];for(let h=0,f=d.length;h<f;h++)u.push(d[h].clone(t));this.morphAttributes[c]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let c=0,u=o.length;c<u;c++){let d=o[c];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let l=e.boundingSphere;return l!==null&&(this.boundingSphere=l.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},w0=new At,Jr=new qc,Ec=new fo,C0=new O,Sc=new O,wc=new O,Cc=new O,Wh=new O,Tc=new O,T0=new O,Dc=new O,Xt=class extends vr{constructor(e=new Cn,t=new us){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Tc.set(0,0,0);for(let l=0,c=s.length;l<c;l++){let u=a[l],d=s[l];u!==0&&(Wh.fromBufferAttribute(d,e),o?Tc.addScaledVector(Wh,u):Tc.addScaledVector(Wh.sub(t),u))}t.add(Tc)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Ec.copy(i.boundingSphere),Ec.applyMatrix4(s),Jr.copy(e.ray).recast(e.near),!(Ec.containsPoint(Jr.origin)===!1&&(Jr.intersectSphere(Ec,C0)===null||Jr.origin.distanceToSquared(C0)>(e.far-e.near)**2))&&(w0.copy(s).invert(),Jr.copy(e.ray).applyMatrix4(w0),!(i.boundingBox!==null&&Jr.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Jr)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,l=s.attributes.position,c=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,h=s.groups,f=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,v=h.length;g<v;g++){let m=h[g],p=o[m.materialIndex],S=Math.max(m.start,f.start),E=Math.min(a.count,Math.min(m.start+m.count,f.start+f.count));for(let b=S,F=E;b<F;b+=3){let T=a.getX(b),C=a.getX(b+1),A=a.getX(b+2);r=Ac(this,p,e,i,c,u,d,T,C,A),r&&(r.faceIndex=Math.floor(b/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),v=Math.min(a.count,f.start+f.count);for(let m=g,p=v;m<p;m+=3){let S=a.getX(m),E=a.getX(m+1),b=a.getX(m+2);r=Ac(this,o,e,i,c,u,d,S,E,b),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(l!==void 0)if(Array.isArray(o))for(let g=0,v=h.length;g<v;g++){let m=h[g],p=o[m.materialIndex],S=Math.max(m.start,f.start),E=Math.min(l.count,Math.min(m.start+m.count,f.start+f.count));for(let b=S,F=E;b<F;b+=3){let T=b,C=b+1,A=b+2;r=Ac(this,p,e,i,c,u,d,T,C,A),r&&(r.faceIndex=Math.floor(b/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),v=Math.min(l.count,f.start+f.count);for(let m=g,p=v;m<p;m+=3){let S=m,E=m+1,b=m+2;r=Ac(this,o,e,i,c,u,d,S,E,b),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function YD(n,e,t,i,r,s,o,a){let l;if(e.side===bn?l=i.intersectTriangle(o,s,r,!0,a):l=i.intersectTriangle(r,s,o,e.side===hr,a),l===null)return null;Dc.copy(a),Dc.applyMatrix4(n.matrixWorld);let c=t.ray.origin.distanceTo(Dc);return c<t.near||c>t.far?null:{distance:c,point:Dc.clone(),object:n}}function Ac(n,e,t,i,r,s,o,a,l,c){n.getVertexPosition(a,Sc),n.getVertexPosition(l,wc),n.getVertexPosition(c,Cc);let u=YD(n,e,t,i,Sc,wc,Cc,T0);if(u){let d=new O;is.getBarycoord(T0,Sc,wc,Cc,d),r&&(u.uv=is.getInterpolatedAttribute(r,a,l,c,d,new Qe)),s&&(u.uv1=is.getInterpolatedAttribute(s,a,l,c,d,new Qe)),o&&(u.normal=is.getInterpolatedAttribute(o,a,l,c,d,new O),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let h={a,b:l,c,normal:new O,materialIndex:0};is.getNormal(Sc,wc,Cc,h.normal),u.face=h,u.barycoord=d}return u}var xa=class n extends Cn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let l=[],c=[],u=[],d=[],h=0,f=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(l),this.setAttribute("position",new Yt(c,3)),this.setAttribute("normal",new Yt(u,3)),this.setAttribute("uv",new Yt(d,2));function g(v,m,p,S,E,b,F,T,C,A,M){let _=b/C,D=F/A,z=b/2,H=F/2,$=T/2,K=C+1,j=A+1,te=0,W=0,le=new O;for(let pe=0;pe<j;pe++){let Ce=pe*D-H;for(let qe=0;qe<K;qe++){let gt=qe*_-z;le[v]=gt*S,le[m]=Ce*E,le[p]=$,c.push(le.x,le.y,le.z),le[v]=0,le[m]=0,le[p]=T>0?1:-1,u.push(le.x,le.y,le.z),d.push(qe/C),d.push(1-pe/A),te+=1}}for(let pe=0;pe<A;pe++)for(let Ce=0;Ce<C;Ce++){let qe=h+Ce+K*pe,gt=h+Ce+K*(pe+1),X=h+(Ce+1)+K*(pe+1),re=h+(Ce+1)+K*pe;l.push(qe,gt,re),l.push(gt,X,re),W+=6}a.addGroup(f,W,M),f+=W,h+=te}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};function ho(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(console.warn("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function an(n){let e={};for(let t=0;t<n.length;t++){let i=ho(n[t]);for(let r in i)e[r]=i[r]}return e}function ZD(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function Mx(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:ot.workingColorSpace}var JD={clone:ho,merge:an},KD=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,QD=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,di=class extends gr{static get type(){return"ShaderMaterial"}constructor(e){super(),this.isShaderMaterial=!0,this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=KD,this.fragmentShader=QD,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=ho(e.uniforms),this.uniformsGroups=ZD(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Jc=class extends vr{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new At,this.projectionMatrix=new At,this.projectionMatrixInverse=new At,this.coordinateSystem=Bi}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorldInverse.copy(this.matrixWorld).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorldInverse.copy(this.matrixWorld).invert()}clone(){return new this.constructor().copy(this)}},cr=new O,D0=new Qe,A0=new Qe,nn=class extends Jc{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Bp*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Sh*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Bp*2*Math.atan(Math.tan(Sh*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){cr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(cr.x,cr.y).multiplyScalar(-e/cr.z),cr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(cr.x,cr.y).multiplyScalar(-e/cr.z)}getViewSize(e,t){return this.getViewBounds(e,D0,A0),t.subVectors(A0,D0)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Sh*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let l=o.fullWidth,c=o.fullHeight;s+=o.offsetX*r/l,t-=o.offsetY*i/c,r*=o.width/l,i*=o.height/c}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}},Ks=-90,Qs=1,Wp=class extends vr{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new nn(Ks,Qs,e,t);r.layers=this.layers,this.add(r);let s=new nn(Ks,Qs,e,t);s.layers=this.layers,this.add(s);let o=new nn(Ks,Qs,e,t);o.layers=this.layers,this.add(o);let a=new nn(Ks,Qs,e,t);a.layers=this.layers,this.add(a);let l=new nn(Ks,Qs,e,t);l.layers=this.layers,this.add(l);let c=new nn(Ks,Qs,e,t);c.layers=this.layers,this.add(c)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,l]=t;for(let c of t)this.remove(c);if(e===Bi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),l.up.set(0,1,0),l.lookAt(0,0,-1);else if(e===Gc)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),l.up.set(0,-1,0),l.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let c of t)this.add(c),c.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,l,c,u]=this.children,d=e.getRenderTarget(),h=e.getActiveCubeFace(),f=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let v=i.texture.generateMipmaps;i.texture.generateMipmaps=!1,e.setRenderTarget(i,0,r),e.render(t,s),e.setRenderTarget(i,1,r),e.render(t,o),e.setRenderTarget(i,2,r),e.render(t,a),e.setRenderTarget(i,3,r),e.render(t,l),e.setRenderTarget(i,4,r),e.render(t,c),i.texture.generateMipmaps=v,e.setRenderTarget(i,5,r),e.render(t,u),e.setRenderTarget(d,h,f),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},Kc=class extends ps{constructor(e,t,i,r,s,o,a,l,c,u){e=e!==void 0?e:[],t=t!==void 0?t:ao,super(e,t,i,r,s,o,a,l,c,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},jp=class extends Wi{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Kc(r,t.mapping,t.wrapS,t.wrapT,t.magFilter,t.minFilter,t.format,t.type,t.anisotropy,t.colorSpace),this.texture.isRenderTargetTexture=!0,this.texture.generateMipmaps=t.generateMipmaps!==void 0?t.generateMipmaps:!1,this.texture.minFilter=t.minFilter!==void 0?t.minFilter:ui}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

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
			`},r=new xa(5,5,5),s=new di({name:"CubemapFromEquirect",uniforms:ho(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:bn,blending:dr});s.uniforms.tEquirect.value=t;let o=new Xt(r,s),a=t.minFilter;return t.minFilter===os&&(t.minFilter=ui),new Wp(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t,i,r){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},jh=new O,eA=new O,tA=new Be,ki=class{constructor(e=new O(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=jh.subVectors(i,t).cross(eA.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(jh),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||tA.getNormalMatrix(e),r=this.coplanarPoint(jh).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Kr=new fo,Ic=new O,ba=class{constructor(e=new ki,t=new ki,i=new ki,r=new ki,s=new ki,o=new ki){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Bi){let i=this.planes,r=e.elements,s=r[0],o=r[1],a=r[2],l=r[3],c=r[4],u=r[5],d=r[6],h=r[7],f=r[8],g=r[9],v=r[10],m=r[11],p=r[12],S=r[13],E=r[14],b=r[15];if(i[0].setComponents(l-s,h-c,m-f,b-p).normalize(),i[1].setComponents(l+s,h+c,m+f,b+p).normalize(),i[2].setComponents(l+o,h+u,m+g,b+S).normalize(),i[3].setComponents(l-o,h-u,m-g,b-S).normalize(),i[4].setComponents(l-a,h-d,m-v,b-E).normalize(),t===Bi)i[5].setComponents(l+a,h+d,m+v,b+E).normalize();else if(t===Gc)i[5].setComponents(a,d,v,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Kr.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Kr.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Kr)}intersectsSprite(e){return Kr.center.set(0,0,0),Kr.radius=.7071067811865476,Kr.applyMatrix4(e.matrixWorld),this.intersectsSphere(Kr)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Ic.x=r.normal.x>0?e.max.x:e.min.x,Ic.y=r.normal.y>0?e.max.y:e.min.y,Ic.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Ic)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};function Ex(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function nA(n){let e=new WeakMap;function t(a,l){let c=a.array,u=a.usage,d=c.byteLength,h=n.createBuffer();n.bindBuffer(l,h),n.bufferData(l,c,u),a.onUploadCallback();let f;if(c instanceof Float32Array)f=n.FLOAT;else if(c instanceof Uint16Array)a.isFloat16BufferAttribute?f=n.HALF_FLOAT:f=n.UNSIGNED_SHORT;else if(c instanceof Int16Array)f=n.SHORT;else if(c instanceof Uint32Array)f=n.UNSIGNED_INT;else if(c instanceof Int32Array)f=n.INT;else if(c instanceof Int8Array)f=n.BYTE;else if(c instanceof Uint8Array)f=n.UNSIGNED_BYTE;else if(c instanceof Uint8ClampedArray)f=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+c);return{buffer:h,type:f,bytesPerElement:c.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,l,c){let u=l.array,d=l.updateRanges;if(n.bindBuffer(c,a),d.length===0)n.bufferSubData(c,0,u);else{d.sort((f,g)=>f.start-g.start);let h=0;for(let f=1;f<d.length;f++){let g=d[h],v=d[f];v.start<=g.start+g.count+1?g.count=Math.max(g.count,v.start+v.count-g.start):(++h,d[h]=v)}d.length=h+1;for(let f=0,g=d.length;f<g;f++){let v=d[f];n.bufferSubData(c,v.start*u.BYTES_PER_ELEMENT,u,v.start,v.count)}l.clearUpdateRanges()}l.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let l=e.get(a);l&&(n.deleteBuffer(l.buffer),e.delete(a))}function o(a,l){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let c=e.get(a);if(c===void 0)e.set(a,t(a,l));else if(c.version<a.version){if(c.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(c.buffer,a,l),c.version=a.version}}return{get:r,remove:s,update:o}}var Qc=class n extends Cn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),l=Math.floor(r),c=a+1,u=l+1,d=e/a,h=t/l,f=[],g=[],v=[],m=[];for(let p=0;p<u;p++){let S=p*h-o;for(let E=0;E<c;E++){let b=E*d-s;g.push(b,-S,0),v.push(0,0,1),m.push(E/a),m.push(1-p/l)}}for(let p=0;p<l;p++)for(let S=0;S<a;S++){let E=S+c*p,b=S+c*(p+1),F=S+1+c*(p+1),T=S+1+c*p;f.push(E,b,T),f.push(b,F,T)}this.setIndex(f),this.setAttribute("position",new Yt(g,3)),this.setAttribute("normal",new Yt(v,3)),this.setAttribute("uv",new Yt(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}},iA=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,rA=`#ifdef USE_ALPHAHASH
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
#endif`,sA=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,oA=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,aA=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,lA=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,cA=`#ifdef USE_AOMAP
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
#endif`,uA=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,dA=`#ifdef USE_BATCHING
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
#endif`,fA=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,hA=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,pA=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,mA=`float G_BlinnPhong_Implicit( ) {
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
} // validated`,gA=`#ifdef USE_IRIDESCENCE
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
#endif`,vA=`#ifdef USE_BUMPMAP
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
#endif`,yA=`#if NUM_CLIPPING_PLANES > 0
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
#endif`,_A=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,xA=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,bA=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,MA=`#if defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#elif defined( USE_COLOR )
	diffuseColor.rgb *= vColor;
#endif`,EA=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR )
	varying vec3 vColor;
#endif`,SA=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec3 vColor;
#endif`,wA=`#if defined( USE_COLOR_ALPHA )
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
#endif`,CA=`#define PI 3.141592653589793
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
} // validated`,TA=`#ifdef ENVMAP_TYPE_CUBE_UV
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
#endif`,DA=`vec3 transformedNormal = objectNormal;
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
#endif`,AA=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,IA=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,RA=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,NA=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,PA="gl_FragColor = linearToOutputTexel( gl_FragColor );",OA=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,FA=`#ifdef USE_ENVMAP
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
#endif`,LA=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
	
#endif`,kA=`#ifdef USE_ENVMAP
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
#endif`,UA=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,VA=`#ifdef USE_ENVMAP
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
#endif`,BA=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,HA=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,zA=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,GA=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,WA=`#ifdef USE_GRADIENTMAP
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
}`,jA=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,$A=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,qA=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,XA=`uniform bool receiveShadow;
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
#endif`,YA=`#ifdef USE_ENVMAP
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
#endif`,ZA=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,JA=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,KA=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,QA=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,eI=`PhysicalMaterial material;
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
#endif`,tI=`struct PhysicalMaterial {
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
}`,nI=`
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
#endif`,iI=`#if defined( RE_IndirectDiffuse )
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
#endif`,rI=`#if defined( RE_IndirectDiffuse )
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,sI=`#if defined( USE_LOGDEPTHBUF )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,oI=`#if defined( USE_LOGDEPTHBUF )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,aI=`#ifdef USE_LOGDEPTHBUF
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,lI=`#ifdef USE_LOGDEPTHBUF
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,cI=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,uI=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,dI=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
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
#endif`,fI=`#if defined( USE_POINTS_UV )
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
#endif`,hI=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,pI=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,mI=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,gI=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,vI=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,yI=`#ifdef USE_MORPHTARGETS
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
#endif`,_I=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,xI=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
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
vec3 nonPerturbedNormal = normal;`,bI=`#ifdef USE_NORMALMAP_OBJECTSPACE
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
#endif`,MI=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,EI=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,SI=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,wI=`#ifdef USE_NORMALMAP
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
#endif`,CI=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,TI=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,DI=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,AI=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,II=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,RI=`vec3 packNormalToRGB( const in vec3 normal ) {
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
}`,NI=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,PI=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,OI=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,FI=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,LI=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,kI=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,UI=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,VI=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,BI=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
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
#endif`,HI=`float getShadowMask() {
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
}`,zI=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,GI=`#ifdef USE_SKINNING
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
#endif`,WI=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,jI=`#ifdef USE_SKINNING
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
#endif`,$I=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,qI=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,XI=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,YI=`#ifndef saturate
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
vec3 CustomToneMapping( vec3 color ) { return color; }`,ZI=`#ifdef USE_TRANSMISSION
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
#endif`,JI=`#ifdef USE_TRANSMISSION
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
#endif`,KI=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,QI=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,e1=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,t1=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,n1=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,i1=`uniform sampler2D t2D;
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
}`,r1=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,s1=`#ifdef ENVMAP_TYPE_CUBE
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
}`,o1=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,a1=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,l1=`#include <common>
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
}`,c1=`#if DEPTH_PACKING == 3200
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
}`,u1=`#define DISTANCE
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
}`,d1=`#define DISTANCE
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
}`,f1=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,h1=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,p1=`uniform float scale;
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
}`,m1=`uniform vec3 diffuse;
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
}`,g1=`#include <common>
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
}`,v1=`uniform vec3 diffuse;
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
}`,y1=`#define LAMBERT
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
}`,_1=`#define LAMBERT
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
}`,x1=`#define MATCAP
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
}`,b1=`#define MATCAP
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
}`,M1=`#define NORMAL
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
}`,E1=`#define NORMAL
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
}`,S1=`#define PHONG
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
}`,w1=`#define PHONG
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
}`,C1=`#define STANDARD
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
}`,T1=`#define STANDARD
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
}`,D1=`#define TOON
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
}`,A1=`#define TOON
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
}`,I1=`uniform float size;
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
}`,R1=`uniform vec3 diffuse;
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
}`,N1=`#include <common>
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
}`,P1=`uniform vec3 color;
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
}`,O1=`uniform float rotation;
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
}`,F1=`uniform vec3 diffuse;
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
}`,Ge={alphahash_fragment:iA,alphahash_pars_fragment:rA,alphamap_fragment:sA,alphamap_pars_fragment:oA,alphatest_fragment:aA,alphatest_pars_fragment:lA,aomap_fragment:cA,aomap_pars_fragment:uA,batching_pars_vertex:dA,batching_vertex:fA,begin_vertex:hA,beginnormal_vertex:pA,bsdfs:mA,iridescence_fragment:gA,bumpmap_pars_fragment:vA,clipping_planes_fragment:yA,clipping_planes_pars_fragment:_A,clipping_planes_pars_vertex:xA,clipping_planes_vertex:bA,color_fragment:MA,color_pars_fragment:EA,color_pars_vertex:SA,color_vertex:wA,common:CA,cube_uv_reflection_fragment:TA,defaultnormal_vertex:DA,displacementmap_pars_vertex:AA,displacementmap_vertex:IA,emissivemap_fragment:RA,emissivemap_pars_fragment:NA,colorspace_fragment:PA,colorspace_pars_fragment:OA,envmap_fragment:FA,envmap_common_pars_fragment:LA,envmap_pars_fragment:kA,envmap_pars_vertex:UA,envmap_physical_pars_fragment:YA,envmap_vertex:VA,fog_vertex:BA,fog_pars_vertex:HA,fog_fragment:zA,fog_pars_fragment:GA,gradientmap_pars_fragment:WA,lightmap_pars_fragment:jA,lights_lambert_fragment:$A,lights_lambert_pars_fragment:qA,lights_pars_begin:XA,lights_toon_fragment:ZA,lights_toon_pars_fragment:JA,lights_phong_fragment:KA,lights_phong_pars_fragment:QA,lights_physical_fragment:eI,lights_physical_pars_fragment:tI,lights_fragment_begin:nI,lights_fragment_maps:iI,lights_fragment_end:rI,logdepthbuf_fragment:sI,logdepthbuf_pars_fragment:oI,logdepthbuf_pars_vertex:aI,logdepthbuf_vertex:lI,map_fragment:cI,map_pars_fragment:uI,map_particle_fragment:dI,map_particle_pars_fragment:fI,metalnessmap_fragment:hI,metalnessmap_pars_fragment:pI,morphinstance_vertex:mI,morphcolor_vertex:gI,morphnormal_vertex:vI,morphtarget_pars_vertex:yI,morphtarget_vertex:_I,normal_fragment_begin:xI,normal_fragment_maps:bI,normal_pars_fragment:MI,normal_pars_vertex:EI,normal_vertex:SI,normalmap_pars_fragment:wI,clearcoat_normal_fragment_begin:CI,clearcoat_normal_fragment_maps:TI,clearcoat_pars_fragment:DI,iridescence_pars_fragment:AI,opaque_fragment:II,packing:RI,premultiplied_alpha_fragment:NI,project_vertex:PI,dithering_fragment:OI,dithering_pars_fragment:FI,roughnessmap_fragment:LI,roughnessmap_pars_fragment:kI,shadowmap_pars_fragment:UI,shadowmap_pars_vertex:VI,shadowmap_vertex:BI,shadowmask_pars_fragment:HI,skinbase_vertex:zI,skinning_pars_vertex:GI,skinning_vertex:WI,skinnormal_vertex:jI,specularmap_fragment:$I,specularmap_pars_fragment:qI,tonemapping_fragment:XI,tonemapping_pars_fragment:YI,transmission_fragment:ZI,transmission_pars_fragment:JI,uv_pars_fragment:KI,uv_pars_vertex:QI,uv_vertex:e1,worldpos_vertex:t1,background_vert:n1,background_frag:i1,backgroundCube_vert:r1,backgroundCube_frag:s1,cube_vert:o1,cube_frag:a1,depth_vert:l1,depth_frag:c1,distanceRGBA_vert:u1,distanceRGBA_frag:d1,equirect_vert:f1,equirect_frag:h1,linedashed_vert:p1,linedashed_frag:m1,meshbasic_vert:g1,meshbasic_frag:v1,meshlambert_vert:y1,meshlambert_frag:_1,meshmatcap_vert:x1,meshmatcap_frag:b1,meshnormal_vert:M1,meshnormal_frag:E1,meshphong_vert:S1,meshphong_frag:w1,meshphysical_vert:C1,meshphysical_frag:T1,meshtoon_vert:D1,meshtoon_frag:A1,points_vert:I1,points_frag:R1,shadow_vert:N1,shadow_frag:P1,sprite_vert:O1,sprite_frag:F1},oe={common:{diffuse:{value:new $e(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Be}},envmap:{envMap:{value:null},envMapRotation:{value:new Be},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Be}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Be}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Be},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Be},normalScale:{value:new Qe(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Be},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Be}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Be}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Be}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new $e(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMap:{value:[]},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotShadowMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMap:{value:[]},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new $e(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0},uvTransform:{value:new Be}},sprite:{diffuse:{value:new $e(16777215)},opacity:{value:1},center:{value:new Qe(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Be},alphaMap:{value:null},alphaMapTransform:{value:new Be},alphaTest:{value:0}}},ci={basic:{uniforms:an([oe.common,oe.specularmap,oe.envmap,oe.aomap,oe.lightmap,oe.fog]),vertexShader:Ge.meshbasic_vert,fragmentShader:Ge.meshbasic_frag},lambert:{uniforms:an([oe.common,oe.specularmap,oe.envmap,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.fog,oe.lights,{emissive:{value:new $e(0)}}]),vertexShader:Ge.meshlambert_vert,fragmentShader:Ge.meshlambert_frag},phong:{uniforms:an([oe.common,oe.specularmap,oe.envmap,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.fog,oe.lights,{emissive:{value:new $e(0)},specular:{value:new $e(1118481)},shininess:{value:30}}]),vertexShader:Ge.meshphong_vert,fragmentShader:Ge.meshphong_frag},standard:{uniforms:an([oe.common,oe.envmap,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.roughnessmap,oe.metalnessmap,oe.fog,oe.lights,{emissive:{value:new $e(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Ge.meshphysical_vert,fragmentShader:Ge.meshphysical_frag},toon:{uniforms:an([oe.common,oe.aomap,oe.lightmap,oe.emissivemap,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.gradientmap,oe.fog,oe.lights,{emissive:{value:new $e(0)}}]),vertexShader:Ge.meshtoon_vert,fragmentShader:Ge.meshtoon_frag},matcap:{uniforms:an([oe.common,oe.bumpmap,oe.normalmap,oe.displacementmap,oe.fog,{matcap:{value:null}}]),vertexShader:Ge.meshmatcap_vert,fragmentShader:Ge.meshmatcap_frag},points:{uniforms:an([oe.points,oe.fog]),vertexShader:Ge.points_vert,fragmentShader:Ge.points_frag},dashed:{uniforms:an([oe.common,oe.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Ge.linedashed_vert,fragmentShader:Ge.linedashed_frag},depth:{uniforms:an([oe.common,oe.displacementmap]),vertexShader:Ge.depth_vert,fragmentShader:Ge.depth_frag},normal:{uniforms:an([oe.common,oe.bumpmap,oe.normalmap,oe.displacementmap,{opacity:{value:1}}]),vertexShader:Ge.meshnormal_vert,fragmentShader:Ge.meshnormal_frag},sprite:{uniforms:an([oe.sprite,oe.fog]),vertexShader:Ge.sprite_vert,fragmentShader:Ge.sprite_frag},background:{uniforms:{uvTransform:{value:new Be},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Ge.background_vert,fragmentShader:Ge.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Be}},vertexShader:Ge.backgroundCube_vert,fragmentShader:Ge.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Ge.cube_vert,fragmentShader:Ge.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Ge.equirect_vert,fragmentShader:Ge.equirect_frag},distanceRGBA:{uniforms:an([oe.common,oe.displacementmap,{referencePosition:{value:new O},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Ge.distanceRGBA_vert,fragmentShader:Ge.distanceRGBA_frag},shadow:{uniforms:an([oe.lights,oe.fog,{color:{value:new $e(0)},opacity:{value:1}}]),vertexShader:Ge.shadow_vert,fragmentShader:Ge.shadow_frag}};ci.physical={uniforms:an([ci.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Be},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Be},clearcoatNormalScale:{value:new Qe(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Be},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Be},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Be},sheen:{value:0},sheenColor:{value:new $e(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Be},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Be},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Be},transmissionSamplerSize:{value:new Qe},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Be},attenuationDistance:{value:0},attenuationColor:{value:new $e(0)},specularColor:{value:new $e(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Be},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Be},anisotropyVector:{value:new Qe},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Be}}]),vertexShader:Ge.meshphysical_vert,fragmentShader:Ge.meshphysical_frag};var Rc={r:0,b:0,g:0},Qr=new cs,L1=new At;function k1(n,e,t,i,r,s,o){let a=new $e(0),l=s===!0?0:1,c,u,d=null,h=0,f=null;function g(S){let E=S.isScene===!0?S.background:null;return E&&E.isTexture&&(E=(S.backgroundBlurriness>0?t:e).get(E)),E}function v(S){let E=!1,b=g(S);b===null?p(a,l):b&&b.isColor&&(p(b,1),E=!0);let F=n.xr.getEnvironmentBlendMode();F==="additive"?i.buffers.color.setClear(0,0,0,1,o):F==="alpha-blend"&&i.buffers.color.setClear(0,0,0,0,o),(n.autoClear||E)&&(i.buffers.depth.setTest(!0),i.buffers.depth.setMask(!0),i.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function m(S,E){let b=g(E);b&&(b.isCubeTexture||b.mapping===hu)?(u===void 0&&(u=new Xt(new xa(1,1,1),new di({name:"BackgroundCubeMaterial",uniforms:ho(ci.backgroundCube.uniforms),vertexShader:ci.backgroundCube.vertexShader,fragmentShader:ci.backgroundCube.fragmentShader,side:bn,depthTest:!1,depthWrite:!1,fog:!1})),u.geometry.deleteAttribute("normal"),u.geometry.deleteAttribute("uv"),u.onBeforeRender=function(F,T,C){this.matrixWorld.copyPosition(C.matrixWorld)},Object.defineProperty(u.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),r.update(u)),Qr.copy(E.backgroundRotation),Qr.x*=-1,Qr.y*=-1,Qr.z*=-1,b.isCubeTexture&&b.isRenderTargetTexture===!1&&(Qr.y*=-1,Qr.z*=-1),u.material.uniforms.envMap.value=b,u.material.uniforms.flipEnvMap.value=b.isCubeTexture&&b.isRenderTargetTexture===!1?-1:1,u.material.uniforms.backgroundBlurriness.value=E.backgroundBlurriness,u.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,u.material.uniforms.backgroundRotation.value.setFromMatrix4(L1.makeRotationFromEuler(Qr)),u.material.toneMapped=ot.getTransfer(b.colorSpace)!==pt,(d!==b||h!==b.version||f!==n.toneMapping)&&(u.material.needsUpdate=!0,d=b,h=b.version,f=n.toneMapping),u.layers.enableAll(),S.unshift(u,u.geometry,u.material,0,0,null)):b&&b.isTexture&&(c===void 0&&(c=new Xt(new Qc(2,2),new di({name:"BackgroundMaterial",uniforms:ho(ci.background.uniforms),vertexShader:ci.background.vertexShader,fragmentShader:ci.background.fragmentShader,side:hr,depthTest:!1,depthWrite:!1,fog:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),r.update(c)),c.material.uniforms.t2D.value=b,c.material.uniforms.backgroundIntensity.value=E.backgroundIntensity,c.material.toneMapped=ot.getTransfer(b.colorSpace)!==pt,b.matrixAutoUpdate===!0&&b.updateMatrix(),c.material.uniforms.uvTransform.value.copy(b.matrix),(d!==b||h!==b.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,d=b,h=b.version,f=n.toneMapping),c.layers.enableAll(),S.unshift(c,c.geometry,c.material,0,0,null))}function p(S,E){S.getRGB(Rc,Mx(n)),i.buffers.color.setClear(Rc.r,Rc.g,Rc.b,E,o)}return{getClearColor:function(){return a},setClearColor:function(S,E=1){a.set(S),l=E,p(a,l)},getClearAlpha:function(){return l},setClearAlpha:function(S){l=S,p(a,l)},render:v,addToRenderList:m}}function U1(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=h(null),s=r,o=!1;function a(_,D,z,H,$){let K=!1,j=d(H,z,D);s!==j&&(s=j,c(s.object)),K=f(_,H,z,$),K&&g(_,H,z,$),$!==null&&e.update($,n.ELEMENT_ARRAY_BUFFER),(K||o)&&(o=!1,b(_,D,z,H),$!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get($).buffer))}function l(){return n.createVertexArray()}function c(_){return n.bindVertexArray(_)}function u(_){return n.deleteVertexArray(_)}function d(_,D,z){let H=z.wireframe===!0,$=i[_.id];$===void 0&&($={},i[_.id]=$);let K=$[D.id];K===void 0&&(K={},$[D.id]=K);let j=K[H];return j===void 0&&(j=h(l()),K[H]=j),j}function h(_){let D=[],z=[],H=[];for(let $=0;$<t;$++)D[$]=0,z[$]=0,H[$]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:D,enabledAttributes:z,attributeDivisors:H,object:_,attributes:{},index:null}}function f(_,D,z,H){let $=s.attributes,K=D.attributes,j=0,te=z.getAttributes();for(let W in te)if(te[W].location>=0){let pe=$[W],Ce=K[W];if(Ce===void 0&&(W==="instanceMatrix"&&_.instanceMatrix&&(Ce=_.instanceMatrix),W==="instanceColor"&&_.instanceColor&&(Ce=_.instanceColor)),pe===void 0||pe.attribute!==Ce||Ce&&pe.data!==Ce.data)return!0;j++}return s.attributesNum!==j||s.index!==H}function g(_,D,z,H){let $={},K=D.attributes,j=0,te=z.getAttributes();for(let W in te)if(te[W].location>=0){let pe=K[W];pe===void 0&&(W==="instanceMatrix"&&_.instanceMatrix&&(pe=_.instanceMatrix),W==="instanceColor"&&_.instanceColor&&(pe=_.instanceColor));let Ce={};Ce.attribute=pe,pe&&pe.data&&(Ce.data=pe.data),$[W]=Ce,j++}s.attributes=$,s.attributesNum=j,s.index=H}function v(){let _=s.newAttributes;for(let D=0,z=_.length;D<z;D++)_[D]=0}function m(_){p(_,0)}function p(_,D){let z=s.newAttributes,H=s.enabledAttributes,$=s.attributeDivisors;z[_]=1,H[_]===0&&(n.enableVertexAttribArray(_),H[_]=1),$[_]!==D&&(n.vertexAttribDivisor(_,D),$[_]=D)}function S(){let _=s.newAttributes,D=s.enabledAttributes;for(let z=0,H=D.length;z<H;z++)D[z]!==_[z]&&(n.disableVertexAttribArray(z),D[z]=0)}function E(_,D,z,H,$,K,j){j===!0?n.vertexAttribIPointer(_,D,z,$,K):n.vertexAttribPointer(_,D,z,H,$,K)}function b(_,D,z,H){v();let $=H.attributes,K=z.getAttributes(),j=D.defaultAttributeValues;for(let te in K){let W=K[te];if(W.location>=0){let le=$[te];if(le===void 0&&(te==="instanceMatrix"&&_.instanceMatrix&&(le=_.instanceMatrix),te==="instanceColor"&&_.instanceColor&&(le=_.instanceColor)),le!==void 0){let pe=le.normalized,Ce=le.itemSize,qe=e.get(le);if(qe===void 0)continue;let gt=qe.buffer,X=qe.type,re=qe.bytesPerElement,be=X===n.INT||X===n.UNSIGNED_INT||le.gpuType===gm;if(le.isInterleavedBufferAttribute){let ce=le.data,Re=ce.stride,ke=le.offset;if(ce.isInstancedInterleavedBuffer){for(let Xe=0;Xe<W.locationSize;Xe++)p(W.location+Xe,ce.meshPerAttribute);_.isInstancedMesh!==!0&&H._maxInstanceCount===void 0&&(H._maxInstanceCount=ce.meshPerAttribute*ce.count)}else for(let Xe=0;Xe<W.locationSize;Xe++)m(W.location+Xe);n.bindBuffer(n.ARRAY_BUFFER,gt);for(let Xe=0;Xe<W.locationSize;Xe++)E(W.location+Xe,Ce/W.locationSize,X,pe,Re*re,(ke+Ce/W.locationSize*Xe)*re,be)}else{if(le.isInstancedBufferAttribute){for(let ce=0;ce<W.locationSize;ce++)p(W.location+ce,le.meshPerAttribute);_.isInstancedMesh!==!0&&H._maxInstanceCount===void 0&&(H._maxInstanceCount=le.meshPerAttribute*le.count)}else for(let ce=0;ce<W.locationSize;ce++)m(W.location+ce);n.bindBuffer(n.ARRAY_BUFFER,gt);for(let ce=0;ce<W.locationSize;ce++)E(W.location+ce,Ce/W.locationSize,X,pe,Ce*re,Ce/W.locationSize*ce*re,be)}}else if(j!==void 0){let pe=j[te];if(pe!==void 0)switch(pe.length){case 2:n.vertexAttrib2fv(W.location,pe);break;case 3:n.vertexAttrib3fv(W.location,pe);break;case 4:n.vertexAttrib4fv(W.location,pe);break;default:n.vertexAttrib1fv(W.location,pe)}}}}S()}function F(){A();for(let _ in i){let D=i[_];for(let z in D){let H=D[z];for(let $ in H)u(H[$].object),delete H[$];delete D[z]}delete i[_]}}function T(_){if(i[_.id]===void 0)return;let D=i[_.id];for(let z in D){let H=D[z];for(let $ in H)u(H[$].object),delete H[$];delete D[z]}delete i[_.id]}function C(_){for(let D in i){let z=i[D];if(z[_.id]===void 0)continue;let H=z[_.id];for(let $ in H)u(H[$].object),delete H[$];delete z[_.id]}}function A(){M(),o=!0,s!==r&&(s=r,c(s.object))}function M(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:A,resetDefaultState:M,dispose:F,releaseStatesOfGeometry:T,releaseStatesOfProgram:C,initAttributes:v,enableAttribute:m,disableUnusedAttributes:S}}function V1(n,e,t){let i;function r(c){i=c}function s(c,u){n.drawArrays(i,c,u),t.update(u,i,1)}function o(c,u,d){d!==0&&(n.drawArraysInstanced(i,c,u,d),t.update(u,i,d))}function a(c,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,u,0,d);let f=0;for(let g=0;g<d;g++)f+=u[g];t.update(f,i,1)}function l(c,u,d,h){if(d===0)return;let f=e.get("WEBGL_multi_draw");if(f===null)for(let g=0;g<c.length;g++)o(c[g],u[g],h[g]);else{f.multiDrawArraysInstancedWEBGL(i,c,0,u,0,h,0,d);let g=0;for(let v=0;v<d;v++)g+=u[v]*h[v];t.update(g,i,1)}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=l}function B1(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let C=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(C.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(C){return!(C!==Kn&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(C){let A=C===Sa&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(C!==Gi&&i.convert(C)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&C!==Vi&&!A)}function l(C){if(C==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";C="mediump"}return C==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let c=t.precision!==void 0?t.precision:"highp",u=l(c);u!==c&&(console.warn("THREE.WebGLRenderer:",c,"not supported, using",u,"instead."),c=u);let d=t.logarithmicDepthBuffer===!0,h=t.reverseDepthBuffer===!0&&e.has("EXT_clip_control"),f=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),v=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),S=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),E=n.getParameter(n.MAX_VARYING_VECTORS),b=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),F=g>0,T=n.getParameter(n.MAX_SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:l,textureFormatReadable:o,textureTypeReadable:a,precision:c,logarithmicDepthBuffer:d,reverseDepthBuffer:h,maxTextures:f,maxVertexTextures:g,maxTextureSize:v,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:S,maxVaryings:E,maxFragmentUniforms:b,vertexTextures:F,maxSamples:T}}function H1(n){let e=this,t=null,i=0,r=!1,s=!1,o=new ki,a=new Be,l={value:null,needsUpdate:!1};this.uniform=l,this.numPlanes=0,this.numIntersection=0,this.init=function(d,h){let f=d.length!==0||h||i!==0||r;return r=h,i=d.length,f},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,h){t=u(d,h,0)},this.setState=function(d,h,f){let g=d.clippingPlanes,v=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):c();else{let S=s?0:i,E=S*4,b=p.clippingState||null;l.value=b,b=u(g,h,E,f);for(let F=0;F!==E;++F)b[F]=t[F];p.clippingState=b,this.numIntersection=v?this.numPlanes:0,this.numPlanes+=S}};function c(){l.value!==t&&(l.value=t,l.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,h,f,g){let v=d!==null?d.length:0,m=null;if(v!==0){if(m=l.value,g!==!0||m===null){let p=f+v*4,S=h.matrixWorldInverse;a.getNormalMatrix(S),(m===null||m.length<p)&&(m=new Float32Array(p));for(let E=0,b=f;E!==v;++E,b+=4)o.copy(d[E]).applyMatrix4(S,a),o.normal.toArray(m,b),m[b+3]=o.constant}l.value=m,l.needsUpdate=!0}return e.numPlanes=v,e.numIntersection=0,m}}function z1(n){let e=new WeakMap;function t(o,a){return a===cp?o.mapping=ao:a===up&&(o.mapping=lo),o}function i(o){if(o&&o.isTexture){let a=o.mapping;if(a===cp||a===up)if(e.has(o)){let l=e.get(o).texture;return t(l,o.mapping)}else{let l=o.image;if(l&&l.height>0){let c=new jp(l.height);return c.fromEquirectangularTexture(n,o),e.set(o,c),o.addEventListener("dispose",r),t(c.texture,o.mapping)}else return null}}return o}function r(o){let a=o.target;a.removeEventListener("dispose",r);let l=e.get(a);l!==void 0&&(e.delete(a),l.dispose())}function s(){e=new WeakMap}return{get:i,dispose:s}}var $p=class extends Jc{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,l=r-t;if(this.view!==null&&this.view.enabled){let c=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=c*this.view.offsetX,o=s+c*this.view.width,a-=u*this.view.offsetY,l=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,l,this.near,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},to=4,I0=[.125,.215,.35,.446,.526,.582],rs=20,$h=new $p,R0=new $e,qh=null,Xh=0,Yh=0,Zh=!1,ts=(1+Math.sqrt(5))/2,eo=1/ts,N0=[new O(-ts,eo,0),new O(ts,eo,0),new O(-eo,0,ts),new O(eo,0,ts),new O(0,ts,-eo),new O(0,ts,eo),new O(-1,1,-1),new O(1,1,-1),new O(-1,1,1),new O(1,1,1)],eu=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._lodPlanes=[],this._sizeLods=[],this._sigmas=[],this._blurMaterial=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._compileMaterial(this._blurMaterial)}fromScene(e,t=0,i=.1,r=100){qh=this._renderer.getRenderTarget(),Xh=this._renderer.getActiveCubeFace(),Yh=this._renderer.getActiveMipmapLevel(),Zh=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(256);let s=this._allocateTargets();return s.depthBuffer=!0,this._sceneToCubeUV(e,i,r,s),t>0&&this._blur(s,0,0,t),this._applyPMREM(s),this._cleanup(s),s}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=F0(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=O0(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose()}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodPlanes.length;e++)this._lodPlanes[e].dispose()}_cleanup(e){this._renderer.setRenderTarget(qh,Xh,Yh),this._renderer.xr.enabled=Zh,e.scissorTest=!1,Nc(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===ao||e.mapping===lo?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),qh=this._renderer.getRenderTarget(),Xh=this._renderer.getActiveCubeFace(),Yh=this._renderer.getActiveMipmapLevel(),Zh=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:ui,minFilter:ui,generateMipmaps:!1,type:Sa,format:Kn,colorSpace:go,depthBuffer:!1},r=P0(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=P0(e,t,i);let{_lodMax:s}=this;({sizeLods:this._sizeLods,lodPlanes:this._lodPlanes,sigmas:this._sigmas}=G1(s)),this._blurMaterial=W1(s,e,t)}return r}_compileMaterial(e){let t=new Xt(this._lodPlanes[0],e);this._renderer.compile(t,$h)}_sceneToCubeUV(e,t,i,r){let a=new nn(90,1,t,i),l=[1,-1,1,1,1,1],c=[1,1,1,-1,-1,-1],u=this._renderer,d=u.autoClear,h=u.toneMapping;u.getClearColor(R0),u.toneMapping=fr,u.autoClear=!1;let f=new us({name:"PMREM.Background",side:bn,depthWrite:!1,depthTest:!1}),g=new Xt(new xa,f),v=!1,m=e.background;m?m.isColor&&(f.color.copy(m),e.background=null,v=!0):(f.color.copy(R0),v=!0);for(let p=0;p<6;p++){let S=p%3;S===0?(a.up.set(0,l[p],0),a.lookAt(c[p],0,0)):S===1?(a.up.set(0,0,l[p]),a.lookAt(0,c[p],0)):(a.up.set(0,l[p],0),a.lookAt(0,0,c[p]));let E=this._cubeSize;Nc(r,S*E,p>2?E:0,E,E),u.setRenderTarget(r),v&&u.render(g,a),u.render(e,a)}g.geometry.dispose(),g.material.dispose(),u.toneMapping=h,u.autoClear=d,e.background=m}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===ao||e.mapping===lo;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=F0()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=O0());let s=r?this._cubemapMaterial:this._equirectMaterial,o=new Xt(this._lodPlanes[0],s),a=s.uniforms;a.envMap.value=e;let l=this._cubeSize;Nc(t,0,0,3*l,2*l),i.setRenderTarget(t),i.render(o,$h)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodPlanes.length;for(let s=1;s<r;s++){let o=Math.sqrt(this._sigmas[s]*this._sigmas[s]-this._sigmas[s-1]*this._sigmas[s-1]),a=N0[(r-s-1)%N0.length];this._blur(e,s-1,s,o,a)}t.autoClear=i}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let l=this._renderer,c=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&console.error("blur direction must be either latitudinal or longitudinal!");let u=3,d=new Xt(this._lodPlanes[r],c),h=c.uniforms,f=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*f):2*Math.PI/(2*rs-1),v=s/g,m=isFinite(s)?1+Math.floor(u*v):rs;m>rs&&console.warn(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${rs}`);let p=[],S=0;for(let C=0;C<rs;++C){let A=C/v,M=Math.exp(-A*A/2);p.push(M),C===0?S+=M:C<m&&(S+=2*M)}for(let C=0;C<p.length;C++)p[C]=p[C]/S;h.envMap.value=e.texture,h.samples.value=m,h.weights.value=p,h.latitudinal.value=o==="latitudinal",a&&(h.poleAxis.value=a);let{_lodMax:E}=this;h.dTheta.value=g,h.mipInt.value=E-i;let b=this._sizeLods[r],F=3*b*(r>E-to?r-E+to:0),T=4*(this._cubeSize-b);Nc(t,F,T,3*b,2*b),l.setRenderTarget(t),l.render(d,$h)}};function G1(n){let e=[],t=[],i=[],r=n,s=n-to+1+I0.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);t.push(a);let l=1/a;o>n-to?l=I0[o-n+to-1]:o===0&&(l=0),i.push(l);let c=1/(a-2),u=-c,d=1+c,h=[u,u,d,u,d,d,u,u,d,d,u,d],f=6,g=6,v=3,m=2,p=1,S=new Float32Array(v*g*f),E=new Float32Array(m*g*f),b=new Float32Array(p*g*f);for(let T=0;T<f;T++){let C=T%3*2/3-1,A=T>2?0:-1,M=[C,A,0,C+2/3,A,0,C+2/3,A+1,0,C,A,0,C+2/3,A+1,0,C,A+1,0];S.set(M,v*g*T),E.set(h,m*g*T);let _=[T,T,T,T,T,T];b.set(_,p*g*T)}let F=new Cn;F.setAttribute("position",new ln(S,v)),F.setAttribute("uv",new ln(E,m)),F.setAttribute("faceIndex",new ln(b,p)),e.push(F),r>to&&r--}return{lodPlanes:e,sizeLods:t,sigmas:i}}function P0(n,e,t){let i=new Wi(n,e,t);return i.texture.mapping=hu,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Nc(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function W1(n,e,t){let i=new Float32Array(rs),r=new O(0,1,0);return new di({name:"SphericalGaussianBlur",defines:{n:rs,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:Mm(),fragmentShader:`

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
		`,blending:dr,depthTest:!1,depthWrite:!1})}function O0(){return new di({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:Mm(),fragmentShader:`

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
		`,blending:dr,depthTest:!1,depthWrite:!1})}function F0(){return new di({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:Mm(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:dr,depthTest:!1,depthWrite:!1})}function Mm(){return`

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
	`}function j1(n){let e=new WeakMap,t=null;function i(a){if(a&&a.isTexture){let l=a.mapping,c=l===cp||l===up,u=l===ao||l===lo;if(c||u){let d=e.get(a),h=d!==void 0?d.texture.pmremVersion:0;if(a.isRenderTargetTexture&&a.pmremVersion!==h)return t===null&&(t=new eu(n)),d=c?t.fromEquirectangular(a,d):t.fromCubemap(a,d),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),d.texture;if(d!==void 0)return d.texture;{let f=a.image;return c&&f&&f.height>0||u&&f&&r(f)?(t===null&&(t=new eu(n)),d=c?t.fromEquirectangular(a):t.fromCubemap(a),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),a.addEventListener("dispose",s),d.texture):null}}}return a}function r(a){let l=0,c=6;for(let u=0;u<c;u++)a[u]!==void 0&&l++;return l===c}function s(a){let l=a.target;l.removeEventListener("dispose",s);let c=e.get(l);c!==void 0&&(e.delete(l),c.dispose())}function o(){e=new WeakMap,t!==null&&(t.dispose(),t=null)}return{get:i,dispose:o}}function $1(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r;switch(i){case"WEBGL_depth_texture":r=n.getExtension("WEBGL_depth_texture")||n.getExtension("MOZ_WEBGL_depth_texture")||n.getExtension("WEBKIT_WEBGL_depth_texture");break;case"EXT_texture_filter_anisotropic":r=n.getExtension("EXT_texture_filter_anisotropic")||n.getExtension("MOZ_EXT_texture_filter_anisotropic")||n.getExtension("WEBKIT_EXT_texture_filter_anisotropic");break;case"WEBGL_compressed_texture_s3tc":r=n.getExtension("WEBGL_compressed_texture_s3tc")||n.getExtension("MOZ_WEBGL_compressed_texture_s3tc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_s3tc");break;case"WEBGL_compressed_texture_pvrtc":r=n.getExtension("WEBGL_compressed_texture_pvrtc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_pvrtc");break;default:r=n.getExtension(i)}return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&ga("THREE.WebGLRenderer: "+i+" extension not supported."),r}}}function q1(n,e,t,i){let r={},s=new WeakMap;function o(d){let h=d.target;h.index!==null&&e.remove(h.index);for(let g in h.attributes)e.remove(h.attributes[g]);for(let g in h.morphAttributes){let v=h.morphAttributes[g];for(let m=0,p=v.length;m<p;m++)e.remove(v[m])}h.removeEventListener("dispose",o),delete r[h.id];let f=s.get(h);f&&(e.remove(f),s.delete(h)),i.releaseStatesOfGeometry(h),h.isInstancedBufferGeometry===!0&&delete h._maxInstanceCount,t.memory.geometries--}function a(d,h){return r[h.id]===!0||(h.addEventListener("dispose",o),r[h.id]=!0,t.memory.geometries++),h}function l(d){let h=d.attributes;for(let g in h)e.update(h[g],n.ARRAY_BUFFER);let f=d.morphAttributes;for(let g in f){let v=f[g];for(let m=0,p=v.length;m<p;m++)e.update(v[m],n.ARRAY_BUFFER)}}function c(d){let h=[],f=d.index,g=d.attributes.position,v=0;if(f!==null){let S=f.array;v=f.version;for(let E=0,b=S.length;E<b;E+=3){let F=S[E+0],T=S[E+1],C=S[E+2];h.push(F,T,T,C,C,F)}}else if(g!==void 0){let S=g.array;v=g.version;for(let E=0,b=S.length/3-1;E<b;E+=3){let F=E+0,T=E+1,C=E+2;h.push(F,T,T,C,C,F)}}else return;let m=new(xx(h)?Zc:Yc)(h,1);m.version=v;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let h=s.get(d);if(h){let f=d.index;f!==null&&h.version<f.version&&c(d)}else c(d);return s.get(d)}return{get:a,update:l,getWireframeAttribute:u}}function X1(n,e,t){let i;function r(h){i=h}let s,o;function a(h){s=h.type,o=h.bytesPerElement}function l(h,f){n.drawElements(i,f,s,h*o),t.update(f,i,1)}function c(h,f,g){g!==0&&(n.drawElementsInstanced(i,f,s,h*o,g),t.update(f,i,g))}function u(h,f,g){if(g===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,h,0,g);let m=0;for(let p=0;p<g;p++)m+=f[p];t.update(m,i,1)}function d(h,f,g,v){if(g===0)return;let m=e.get("WEBGL_multi_draw");if(m===null)for(let p=0;p<h.length;p++)c(h[p]/o,f[p],v[p]);else{m.multiDrawElementsInstancedWEBGL(i,f,0,s,h,0,v,0,g);let p=0;for(let S=0;S<g;S++)p+=f[S]*v[S];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=l,this.renderInstances=c,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function Y1(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:console.error("THREE.WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function Z1(n,e,t){let i=new WeakMap,r=new mt;function s(o,a,l){let c=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,h=i.get(a);if(h===void 0||h.count!==d){let _=function(){A.dispose(),i.delete(a),a.removeEventListener("dispose",_)};var f=_;h!==void 0&&h.texture.dispose();let g=a.morphAttributes.position!==void 0,v=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],S=a.morphAttributes.normal||[],E=a.morphAttributes.color||[],b=0;g===!0&&(b=1),v===!0&&(b=2),m===!0&&(b=3);let F=a.attributes.position.count*b,T=1;F>e.maxTextureSize&&(T=Math.ceil(F/e.maxTextureSize),F=e.maxTextureSize);let C=new Float32Array(F*T*4*d),A=new $c(C,F,T,d);A.type=Vi,A.needsUpdate=!0;let M=b*4;for(let D=0;D<d;D++){let z=p[D],H=S[D],$=E[D],K=F*T*4*D;for(let j=0;j<z.count;j++){let te=j*M;g===!0&&(r.fromBufferAttribute(z,j),C[K+te+0]=r.x,C[K+te+1]=r.y,C[K+te+2]=r.z,C[K+te+3]=0),v===!0&&(r.fromBufferAttribute(H,j),C[K+te+4]=r.x,C[K+te+5]=r.y,C[K+te+6]=r.z,C[K+te+7]=0),m===!0&&(r.fromBufferAttribute($,j),C[K+te+8]=r.x,C[K+te+9]=r.y,C[K+te+10]=r.z,C[K+te+11]=$.itemSize===4?r.w:1)}}h={count:d,texture:A,size:new Qe(F,T)},i.set(a,h),a.addEventListener("dispose",_)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)l.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<c.length;m++)g+=c[m];let v=a.morphTargetsRelative?1:1-g;l.getUniforms().setValue(n,"morphTargetBaseInfluence",v),l.getUniforms().setValue(n,"morphTargetInfluences",c)}l.getUniforms().setValue(n,"morphTargetsTexture",h.texture,t),l.getUniforms().setValue(n,"morphTargetsTextureSize",h.size)}return{update:s}}function J1(n,e,t,i){let r=new WeakMap;function s(l){let c=i.render.frame,u=l.geometry,d=e.get(l,u);if(r.get(d)!==c&&(e.update(d),r.set(d,c)),l.isInstancedMesh&&(l.hasEventListener("dispose",a)===!1&&l.addEventListener("dispose",a),r.get(l)!==c&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),r.set(l,c))),l.isSkinnedMesh){let h=l.skeleton;r.get(h)!==c&&(h.update(),r.set(h,c))}return d}function o(){r=new WeakMap}function a(l){let c=l.target;c.removeEventListener("dispose",a),t.remove(c.instanceMatrix),c.instanceColor!==null&&t.remove(c.instanceColor)}return{update:s,dispose:o}}var tu=class extends ps{constructor(e,t,i,r,s,o,a,l,c,u=io){if(u!==io&&u!==uo)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");i===void 0&&u===io&&(i=as),i===void 0&&u===uo&&(i=co),super(null,r,s,o,a,l,u,i,c),this.isDepthTexture=!0,this.image={width:e,height:t},this.magFilter=a!==void 0?a:Qn,this.minFilter=l!==void 0?l:Qn,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Sx=new ps,L0=new tu(1,1),wx=new $c,Cx=new Gp,Tx=new Kc,k0=[],U0=[],V0=new Float32Array(16),B0=new Float32Array(9),H0=new Float32Array(4);function vo(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=k0[r];if(s===void 0&&(s=new Float32Array(r),k0[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Gt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Wt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function mu(n,e){let t=U0[e];t===void 0&&(t=new Int32Array(e),U0[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function K1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function Q1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Gt(t,e))return;n.uniform2fv(this.addr,e),Wt(t,e)}}function eR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Gt(t,e))return;n.uniform3fv(this.addr,e),Wt(t,e)}}function tR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Gt(t,e))return;n.uniform4fv(this.addr,e),Wt(t,e)}}function nR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Gt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Wt(t,e)}else{if(Gt(t,i))return;H0.set(i),n.uniformMatrix2fv(this.addr,!1,H0),Wt(t,i)}}function iR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Gt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Wt(t,e)}else{if(Gt(t,i))return;B0.set(i),n.uniformMatrix3fv(this.addr,!1,B0),Wt(t,i)}}function rR(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Gt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Wt(t,e)}else{if(Gt(t,i))return;V0.set(i),n.uniformMatrix4fv(this.addr,!1,V0),Wt(t,i)}}function sR(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function oR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Gt(t,e))return;n.uniform2iv(this.addr,e),Wt(t,e)}}function aR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Gt(t,e))return;n.uniform3iv(this.addr,e),Wt(t,e)}}function lR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Gt(t,e))return;n.uniform4iv(this.addr,e),Wt(t,e)}}function cR(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function uR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Gt(t,e))return;n.uniform2uiv(this.addr,e),Wt(t,e)}}function dR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Gt(t,e))return;n.uniform3uiv(this.addr,e),Wt(t,e)}}function fR(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Gt(t,e))return;n.uniform4uiv(this.addr,e),Wt(t,e)}}function hR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(L0.compareFunction=_x,s=L0):s=Sx,t.setTexture2D(e||s,r)}function pR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||Cx,r)}function mR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||Tx,r)}function gR(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||wx,r)}function vR(n){switch(n){case 5126:return K1;case 35664:return Q1;case 35665:return eR;case 35666:return tR;case 35674:return nR;case 35675:return iR;case 35676:return rR;case 5124:case 35670:return sR;case 35667:case 35671:return oR;case 35668:case 35672:return aR;case 35669:case 35673:return lR;case 5125:return cR;case 36294:return uR;case 36295:return dR;case 36296:return fR;case 35678:case 36198:case 36298:case 36306:case 35682:return hR;case 35679:case 36299:case 36307:return pR;case 35680:case 36300:case 36308:case 36293:return mR;case 36289:case 36303:case 36311:case 36292:return gR}}function yR(n,e){n.uniform1fv(this.addr,e)}function _R(n,e){let t=vo(e,this.size,2);n.uniform2fv(this.addr,t)}function xR(n,e){let t=vo(e,this.size,3);n.uniform3fv(this.addr,t)}function bR(n,e){let t=vo(e,this.size,4);n.uniform4fv(this.addr,t)}function MR(n,e){let t=vo(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function ER(n,e){let t=vo(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function SR(n,e){let t=vo(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function wR(n,e){n.uniform1iv(this.addr,e)}function CR(n,e){n.uniform2iv(this.addr,e)}function TR(n,e){n.uniform3iv(this.addr,e)}function DR(n,e){n.uniform4iv(this.addr,e)}function AR(n,e){n.uniform1uiv(this.addr,e)}function IR(n,e){n.uniform2uiv(this.addr,e)}function RR(n,e){n.uniform3uiv(this.addr,e)}function NR(n,e){n.uniform4uiv(this.addr,e)}function PR(n,e,t){let i=this.cache,r=e.length,s=mu(t,r);Gt(i,s)||(n.uniform1iv(this.addr,s),Wt(i,s));for(let o=0;o!==r;++o)t.setTexture2D(e[o]||Sx,s[o])}function OR(n,e,t){let i=this.cache,r=e.length,s=mu(t,r);Gt(i,s)||(n.uniform1iv(this.addr,s),Wt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||Cx,s[o])}function FR(n,e,t){let i=this.cache,r=e.length,s=mu(t,r);Gt(i,s)||(n.uniform1iv(this.addr,s),Wt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||Tx,s[o])}function LR(n,e,t){let i=this.cache,r=e.length,s=mu(t,r);Gt(i,s)||(n.uniform1iv(this.addr,s),Wt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||wx,s[o])}function kR(n){switch(n){case 5126:return yR;case 35664:return _R;case 35665:return xR;case 35666:return bR;case 35674:return MR;case 35675:return ER;case 35676:return SR;case 5124:case 35670:return wR;case 35667:case 35671:return CR;case 35668:case 35672:return TR;case 35669:case 35673:return DR;case 5125:return AR;case 36294:return IR;case 36295:return RR;case 36296:return NR;case 35678:case 36198:case 36298:case 36306:case 35682:return PR;case 35679:case 36299:case 36307:return OR;case 35680:case 36300:case 36308:case 36293:return FR;case 36289:case 36303:case 36311:case 36292:return LR}}var qp=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=vR(t.type)}},Xp=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=kR(t.type)}},Yp=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Jh=/(\w+)(\])?(\[|\.)?/g;function z0(n,e){n.seq.push(e),n.map[e.id]=e}function UR(n,e,t){let i=n.name,r=i.length;for(Jh.lastIndex=0;;){let s=Jh.exec(i),o=Jh.lastIndex,a=s[1],l=s[2]==="]",c=s[3];if(l&&(a=a|0),c===void 0||c==="["&&o+2===r){z0(t,c===void 0?new qp(a,n,e):new Xp(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new Yp(a),z0(t,d)),t=d}}}var so=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let r=0;r<i;++r){let s=e.getActiveUniform(t,r),o=e.getUniformLocation(t,s.name);UR(s,o,this)}}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],l=i[a.id];l.needsUpdate!==!1&&a.setValue(e,l.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function G0(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var VR=37297,BR=0;function HR(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var W0=new Be;function zR(n){ot._getMatrix(W0,ot.workingColorSpace,n);let e=`mat3( ${W0.elements.map(t=>t.toFixed(4))} )`;switch(ot.getTransfer(n)){case pu:return[e,"LinearTransferOETF"];case pt:return[e,"sRGBTransferOETF"];default:return console.warn("THREE.WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function j0(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),r=n.getShaderInfoLog(e).trim();if(i&&r==="")return"";let s=/ERROR: 0:(\d+)/.exec(r);if(s){let o=parseInt(s[1]);return t.toUpperCase()+`

`+r+`

`+HR(n.getShaderSource(e),o)}else return r}function GR(n,e){let t=zR(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}function WR(n,e){let t;switch(e){case mD:t="Linear";break;case gD:t="Reinhard";break;case vD:t="Cineon";break;case yD:t="ACESFilmic";break;case xD:t="AgX";break;case bD:t="Neutral";break;case _D:t="Custom";break;default:console.warn("THREE.WebGLProgram: Unsupported toneMapping:",e),t="Linear"}return"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Pc=new O;function jR(){ot.getLuminanceCoefficients(Pc);let n=Pc.x.toFixed(4),e=Pc.y.toFixed(4),t=Pc.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function $R(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(va).join(`
`)}function qR(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function XR(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function va(n){return n!==""}function $0(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function q0(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var YR=/^[ \t]*#include +<([\w\d./]+)>/gm;function Zp(n){return n.replace(YR,JR)}var ZR=new Map;function JR(n,e){let t=Ge[e];if(t===void 0){let i=ZR.get(e);if(i!==void 0)t=Ge[i],console.warn('THREE.WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Zp(t)}var KR=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function X0(n){return n.replace(KR,QR)}function QR(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function Y0(n){let e=`precision ${n.precision} float;
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
#define LOW_PRECISION`),e}function eN(n){let e="SHADOWMAP_TYPE_BASIC";return n.shadowMapType===ox?e="SHADOWMAP_TYPE_PCF":n.shadowMapType===XT?e="SHADOWMAP_TYPE_PCF_SOFT":n.shadowMapType===Li&&(e="SHADOWMAP_TYPE_VSM"),e}function tN(n){let e="ENVMAP_TYPE_CUBE";if(n.envMap)switch(n.envMapMode){case ao:case lo:e="ENVMAP_TYPE_CUBE";break;case hu:e="ENVMAP_TYPE_CUBE_UV";break}return e}function nN(n){let e="ENVMAP_MODE_REFLECTION";return n.envMap&&n.envMapMode===lo&&(e="ENVMAP_MODE_REFRACTION"),e}function iN(n){let e="ENVMAP_BLENDING_NONE";if(n.envMap)switch(n.combine){case ax:e="ENVMAP_BLENDING_MULTIPLY";break;case hD:e="ENVMAP_BLENDING_MIX";break;case pD:e="ENVMAP_BLENDING_ADD";break}return e}function rN(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function sN(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,l=eN(t),c=tN(t),u=nN(t),d=iN(t),h=rN(t),f=$R(t),g=qR(s),v=r.createProgram(),m,p,S=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(va).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(va).join(`
`),p.length>0&&(p+=`
`)):(m=[Y0(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(va).join(`
`),p=[Y0(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+c:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",h?"#define CUBEUV_TEXEL_WIDTH "+h.texelWidth:"",h?"#define CUBEUV_TEXEL_HEIGHT "+h.texelHeight:"",h?"#define CUBEUV_MAX_MIP "+h.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor||t.batchingColor?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==fr?"#define TONE_MAPPING":"",t.toneMapping!==fr?Ge.tonemapping_pars_fragment:"",t.toneMapping!==fr?WR("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Ge.colorspace_pars_fragment,GR("linearToOutputTexel",t.outputColorSpace),jR(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(va).join(`
`)),o=Zp(o),o=$0(o,t),o=q0(o,t),a=Zp(a),a=$0(a,t),a=q0(a,t),o=X0(o),a=X0(a),t.isRawShaderMaterial!==!0&&(S=`#version 300 es
`,m=[f,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===c0?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===c0?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let E=S+m+o,b=S+p+a,F=G0(r,r.VERTEX_SHADER,E),T=G0(r,r.FRAGMENT_SHADER,b);r.attachShader(v,F),r.attachShader(v,T),t.index0AttributeName!==void 0?r.bindAttribLocation(v,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(v,0,"position"),r.linkProgram(v);function C(D){if(n.debug.checkShaderErrors){let z=r.getProgramInfoLog(v).trim(),H=r.getShaderInfoLog(F).trim(),$=r.getShaderInfoLog(T).trim(),K=!0,j=!0;if(r.getProgramParameter(v,r.LINK_STATUS)===!1)if(K=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,v,F,T);else{let te=j0(r,F,"vertex"),W=j0(r,T,"fragment");console.error("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(v,r.VALIDATE_STATUS)+`

Material Name: `+D.name+`
Material Type: `+D.type+`

Program Info Log: `+z+`
`+te+`
`+W)}else z!==""?console.warn("THREE.WebGLProgram: Program Info Log:",z):(H===""||$==="")&&(j=!1);j&&(D.diagnostics={runnable:K,programLog:z,vertexShader:{log:H,prefix:m},fragmentShader:{log:$,prefix:p}})}r.deleteShader(F),r.deleteShader(T),A=new so(r,v),M=XR(r,v)}let A;this.getUniforms=function(){return A===void 0&&C(this),A};let M;this.getAttributes=function(){return M===void 0&&C(this),M};let _=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return _===!1&&(_=r.getProgramParameter(v,VR)),_},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(v),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=BR++,this.cacheKey=e,this.usedTimes=1,this.program=v,this.vertexShader=F,this.fragmentShader=T,this}var oN=0,Jp=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Kp(e),t.set(e,i)),i}},Kp=class{constructor(e){this.id=oN++,this.code=e,this.usedTimes=0}};function aN(n,e,t,i,r,s,o){let a=new Xc,l=new Jp,c=new Set,u=[],d=r.logarithmicDepthBuffer,h=r.vertexTextures,f=r.precision,g={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distanceRGBA",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function v(M){return c.add(M),M===0?"uv":`uv${M}`}function m(M,_,D,z,H){let $=z.fog,K=H.geometry,j=M.isMeshStandardMaterial?z.environment:null,te=(M.isMeshStandardMaterial?t:e).get(M.envMap||j),W=te&&te.mapping===hu?te.image.height:null,le=g[M.type];M.precision!==null&&(f=r.getMaxPrecision(M.precision),f!==M.precision&&console.warn("THREE.WebGLProgram.getParameters:",M.precision,"not supported, using",f,"instead."));let pe=K.morphAttributes.position||K.morphAttributes.normal||K.morphAttributes.color,Ce=pe!==void 0?pe.length:0,qe=0;K.morphAttributes.position!==void 0&&(qe=1),K.morphAttributes.normal!==void 0&&(qe=2),K.morphAttributes.color!==void 0&&(qe=3);let gt,X,re,be;if(le){let ut=ci[le];gt=ut.vertexShader,X=ut.fragmentShader}else gt=M.vertexShader,X=M.fragmentShader,l.update(M),re=l.getVertexShaderID(M),be=l.getFragmentShaderID(M);let ce=n.getRenderTarget(),Re=n.state.buffers.depth.getReversed(),ke=H.isInstancedMesh===!0,Xe=H.isBatchedMesh===!0,wt=!!M.map,rt=!!M.matcap,Ft=!!te,P=!!M.aoMap,Tn=!!M.lightMap,et=!!M.bumpMap,tt=!!M.normalMap,Ae=!!M.displacementMap,xt=!!M.emissiveMap,Te=!!M.metalnessMap,w=!!M.roughnessMap,y=M.anisotropy>0,k=M.clearcoat>0,Y=M.dispersion>0,ee=M.iridescence>0,q=M.sheen>0,Me=M.transmission>0,ue=y&&!!M.anisotropyMap,me=k&&!!M.clearcoatMap,st=k&&!!M.clearcoatNormalMap,ne=k&&!!M.clearcoatRoughnessMap,ge=ee&&!!M.iridescenceMap,Ie=ee&&!!M.iridescenceThicknessMap,Pe=q&&!!M.sheenColorMap,ve=q&&!!M.sheenRoughnessMap,nt=!!M.specularMap,He=!!M.specularColorMap,vt=!!M.specularIntensityMap,I=Me&&!!M.transmissionMap,ae=Me&&!!M.thicknessMap,G=!!M.gradientMap,Z=!!M.alphaMap,he=M.alphaTest>0,de=!!M.alphaHash,Ue=!!M.extensions,It=fr;M.toneMapped&&(ce===null||ce.isXRRenderTarget===!0)&&(It=n.toneMapping);let Zt={shaderID:le,shaderType:M.type,shaderName:M.name,vertexShader:gt,fragmentShader:X,defines:M.defines,customVertexShaderID:re,customFragmentShaderID:be,isRawShaderMaterial:M.isRawShaderMaterial===!0,glslVersion:M.glslVersion,precision:f,batching:Xe,batchingColor:Xe&&H._colorsTexture!==null,instancing:ke,instancingColor:ke&&H.instanceColor!==null,instancingMorph:ke&&H.morphTexture!==null,supportsVertexTextures:h,outputColorSpace:ce===null?n.outputColorSpace:ce.isXRRenderTarget===!0?ce.texture.colorSpace:go,alphaToCoverage:!!M.alphaToCoverage,map:wt,matcap:rt,envMap:Ft,envMapMode:Ft&&te.mapping,envMapCubeUVHeight:W,aoMap:P,lightMap:Tn,bumpMap:et,normalMap:tt,displacementMap:h&&Ae,emissiveMap:xt,normalMapObjectSpace:tt&&M.normalMapType===wD,normalMapTangentSpace:tt&&M.normalMapType===yx,metalnessMap:Te,roughnessMap:w,anisotropy:y,anisotropyMap:ue,clearcoat:k,clearcoatMap:me,clearcoatNormalMap:st,clearcoatRoughnessMap:ne,dispersion:Y,iridescence:ee,iridescenceMap:ge,iridescenceThicknessMap:Ie,sheen:q,sheenColorMap:Pe,sheenRoughnessMap:ve,specularMap:nt,specularColorMap:He,specularIntensityMap:vt,transmission:Me,transmissionMap:I,thicknessMap:ae,gradientMap:G,opaque:M.transparent===!1&&M.blending===no&&M.alphaToCoverage===!1,alphaMap:Z,alphaTest:he,alphaHash:de,combine:M.combine,mapUv:wt&&v(M.map.channel),aoMapUv:P&&v(M.aoMap.channel),lightMapUv:Tn&&v(M.lightMap.channel),bumpMapUv:et&&v(M.bumpMap.channel),normalMapUv:tt&&v(M.normalMap.channel),displacementMapUv:Ae&&v(M.displacementMap.channel),emissiveMapUv:xt&&v(M.emissiveMap.channel),metalnessMapUv:Te&&v(M.metalnessMap.channel),roughnessMapUv:w&&v(M.roughnessMap.channel),anisotropyMapUv:ue&&v(M.anisotropyMap.channel),clearcoatMapUv:me&&v(M.clearcoatMap.channel),clearcoatNormalMapUv:st&&v(M.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:ne&&v(M.clearcoatRoughnessMap.channel),iridescenceMapUv:ge&&v(M.iridescenceMap.channel),iridescenceThicknessMapUv:Ie&&v(M.iridescenceThicknessMap.channel),sheenColorMapUv:Pe&&v(M.sheenColorMap.channel),sheenRoughnessMapUv:ve&&v(M.sheenRoughnessMap.channel),specularMapUv:nt&&v(M.specularMap.channel),specularColorMapUv:He&&v(M.specularColorMap.channel),specularIntensityMapUv:vt&&v(M.specularIntensityMap.channel),transmissionMapUv:I&&v(M.transmissionMap.channel),thicknessMapUv:ae&&v(M.thicknessMap.channel),alphaMapUv:Z&&v(M.alphaMap.channel),vertexTangents:!!K.attributes.tangent&&(tt||y),vertexColors:M.vertexColors,vertexAlphas:M.vertexColors===!0&&!!K.attributes.color&&K.attributes.color.itemSize===4,pointsUvs:H.isPoints===!0&&!!K.attributes.uv&&(wt||Z),fog:!!$,useFog:M.fog===!0,fogExp2:!!$&&$.isFogExp2,flatShading:M.flatShading===!0,sizeAttenuation:M.sizeAttenuation===!0,logarithmicDepthBuffer:d,reverseDepthBuffer:Re,skinning:H.isSkinnedMesh===!0,morphTargets:K.morphAttributes.position!==void 0,morphNormals:K.morphAttributes.normal!==void 0,morphColors:K.morphAttributes.color!==void 0,morphTargetsCount:Ce,morphTextureStride:qe,numDirLights:_.directional.length,numPointLights:_.point.length,numSpotLights:_.spot.length,numSpotLightMaps:_.spotLightMap.length,numRectAreaLights:_.rectArea.length,numHemiLights:_.hemi.length,numDirLightShadows:_.directionalShadowMap.length,numPointLightShadows:_.pointShadowMap.length,numSpotLightShadows:_.spotShadowMap.length,numSpotLightShadowsWithMaps:_.numSpotLightShadowsWithMaps,numLightProbes:_.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:M.dithering,shadowMapEnabled:n.shadowMap.enabled&&D.length>0,shadowMapType:n.shadowMap.type,toneMapping:It,decodeVideoTexture:wt&&M.map.isVideoTexture===!0&&ot.getTransfer(M.map.colorSpace)===pt,decodeVideoTextureEmissive:xt&&M.emissiveMap.isVideoTexture===!0&&ot.getTransfer(M.emissiveMap.colorSpace)===pt,premultipliedAlpha:M.premultipliedAlpha,doubleSided:M.side===Ui,flipSided:M.side===bn,useDepthPacking:M.depthPacking>=0,depthPacking:M.depthPacking||0,index0AttributeName:M.index0AttributeName,extensionClipCullDistance:Ue&&M.extensions.clipCullDistance===!0&&i.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(Ue&&M.extensions.multiDraw===!0||Xe)&&i.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:i.has("KHR_parallel_shader_compile"),customProgramCacheKey:M.customProgramCacheKey()};return Zt.vertexUv1s=c.has(1),Zt.vertexUv2s=c.has(2),Zt.vertexUv3s=c.has(3),c.clear(),Zt}function p(M){let _=[];if(M.shaderID?_.push(M.shaderID):(_.push(M.customVertexShaderID),_.push(M.customFragmentShaderID)),M.defines!==void 0)for(let D in M.defines)_.push(D),_.push(M.defines[D]);return M.isRawShaderMaterial===!1&&(S(_,M),E(_,M),_.push(n.outputColorSpace)),_.push(M.customProgramCacheKey),_.join()}function S(M,_){M.push(_.precision),M.push(_.outputColorSpace),M.push(_.envMapMode),M.push(_.envMapCubeUVHeight),M.push(_.mapUv),M.push(_.alphaMapUv),M.push(_.lightMapUv),M.push(_.aoMapUv),M.push(_.bumpMapUv),M.push(_.normalMapUv),M.push(_.displacementMapUv),M.push(_.emissiveMapUv),M.push(_.metalnessMapUv),M.push(_.roughnessMapUv),M.push(_.anisotropyMapUv),M.push(_.clearcoatMapUv),M.push(_.clearcoatNormalMapUv),M.push(_.clearcoatRoughnessMapUv),M.push(_.iridescenceMapUv),M.push(_.iridescenceThicknessMapUv),M.push(_.sheenColorMapUv),M.push(_.sheenRoughnessMapUv),M.push(_.specularMapUv),M.push(_.specularColorMapUv),M.push(_.specularIntensityMapUv),M.push(_.transmissionMapUv),M.push(_.thicknessMapUv),M.push(_.combine),M.push(_.fogExp2),M.push(_.sizeAttenuation),M.push(_.morphTargetsCount),M.push(_.morphAttributeCount),M.push(_.numDirLights),M.push(_.numPointLights),M.push(_.numSpotLights),M.push(_.numSpotLightMaps),M.push(_.numHemiLights),M.push(_.numRectAreaLights),M.push(_.numDirLightShadows),M.push(_.numPointLightShadows),M.push(_.numSpotLightShadows),M.push(_.numSpotLightShadowsWithMaps),M.push(_.numLightProbes),M.push(_.shadowMapType),M.push(_.toneMapping),M.push(_.numClippingPlanes),M.push(_.numClipIntersection),M.push(_.depthPacking)}function E(M,_){a.disableAll(),_.supportsVertexTextures&&a.enable(0),_.instancing&&a.enable(1),_.instancingColor&&a.enable(2),_.instancingMorph&&a.enable(3),_.matcap&&a.enable(4),_.envMap&&a.enable(5),_.normalMapObjectSpace&&a.enable(6),_.normalMapTangentSpace&&a.enable(7),_.clearcoat&&a.enable(8),_.iridescence&&a.enable(9),_.alphaTest&&a.enable(10),_.vertexColors&&a.enable(11),_.vertexAlphas&&a.enable(12),_.vertexUv1s&&a.enable(13),_.vertexUv2s&&a.enable(14),_.vertexUv3s&&a.enable(15),_.vertexTangents&&a.enable(16),_.anisotropy&&a.enable(17),_.alphaHash&&a.enable(18),_.batching&&a.enable(19),_.dispersion&&a.enable(20),_.batchingColor&&a.enable(21),M.push(a.mask),a.disableAll(),_.fog&&a.enable(0),_.useFog&&a.enable(1),_.flatShading&&a.enable(2),_.logarithmicDepthBuffer&&a.enable(3),_.reverseDepthBuffer&&a.enable(4),_.skinning&&a.enable(5),_.morphTargets&&a.enable(6),_.morphNormals&&a.enable(7),_.morphColors&&a.enable(8),_.premultipliedAlpha&&a.enable(9),_.shadowMapEnabled&&a.enable(10),_.doubleSided&&a.enable(11),_.flipSided&&a.enable(12),_.useDepthPacking&&a.enable(13),_.dithering&&a.enable(14),_.transmission&&a.enable(15),_.sheen&&a.enable(16),_.opaque&&a.enable(17),_.pointsUvs&&a.enable(18),_.decodeVideoTexture&&a.enable(19),_.decodeVideoTextureEmissive&&a.enable(20),_.alphaToCoverage&&a.enable(21),M.push(a.mask)}function b(M){let _=g[M.type],D;if(_){let z=ci[_];D=JD.clone(z.uniforms)}else D=M.uniforms;return D}function F(M,_){let D;for(let z=0,H=u.length;z<H;z++){let $=u[z];if($.cacheKey===_){D=$,++D.usedTimes;break}}return D===void 0&&(D=new sN(n,_,M,s),u.push(D)),D}function T(M){if(--M.usedTimes===0){let _=u.indexOf(M);u[_]=u[u.length-1],u.pop(),M.destroy()}}function C(M){l.remove(M)}function A(){l.dispose()}return{getParameters:m,getProgramCacheKey:p,getUniforms:b,acquireProgram:F,releaseProgram:T,releaseShaderCache:C,programs:u,dispose:A}}function lN(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,l){n.get(o)[a]=l}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function cN(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.z!==e.z?n.z-e.z:n.id-e.id}function Z0(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function J0(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d,h,f,g,v,m){let p=n[e];return p===void 0?(p={id:d.id,object:d,geometry:h,material:f,groupOrder:g,renderOrder:d.renderOrder,z:v,group:m},n[e]=p):(p.id=d.id,p.object=d,p.geometry=h,p.material=f,p.groupOrder=g,p.renderOrder=d.renderOrder,p.z=v,p.group=m),e++,p}function a(d,h,f,g,v,m){let p=o(d,h,f,g,v,m);f.transmission>0?i.push(p):f.transparent===!0?r.push(p):t.push(p)}function l(d,h,f,g,v,m){let p=o(d,h,f,g,v,m);f.transmission>0?i.unshift(p):f.transparent===!0?r.unshift(p):t.unshift(p)}function c(d,h){t.length>1&&t.sort(d||cN),i.length>1&&i.sort(h||Z0),r.length>1&&r.sort(h||Z0)}function u(){for(let d=e,h=n.length;d<h;d++){let f=n[d];if(f.id===null)break;f.id=null,f.object=null,f.geometry=null,f.material=null,f.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:a,unshift:l,finish:u,sort:c}}function uN(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new J0,n.set(i,[o])):r>=s.length?(o=new J0,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function dN(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new O,color:new $e};break;case"SpotLight":t={position:new O,direction:new O,color:new $e,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new O,color:new $e,distance:0,decay:0};break;case"HemisphereLight":t={direction:new O,skyColor:new $e,groundColor:new $e};break;case"RectAreaLight":t={color:new $e,position:new O,halfWidth:new O,halfHeight:new O};break}return n[e.id]=t,t}}}function fN(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Qe};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Qe};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Qe,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var hN=0;function pN(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function mN(n){let e=new dN,t=fN(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let c=0;c<9;c++)i.probe.push(new O);let r=new O,s=new At,o=new At;function a(c){let u=0,d=0,h=0;for(let M=0;M<9;M++)i.probe[M].set(0,0,0);let f=0,g=0,v=0,m=0,p=0,S=0,E=0,b=0,F=0,T=0,C=0;c.sort(pN);for(let M=0,_=c.length;M<_;M++){let D=c[M],z=D.color,H=D.intensity,$=D.distance,K=D.shadow&&D.shadow.map?D.shadow.map.texture:null;if(D.isAmbientLight)u+=z.r*H,d+=z.g*H,h+=z.b*H;else if(D.isLightProbe){for(let j=0;j<9;j++)i.probe[j].addScaledVector(D.sh.coefficients[j],H);C++}else if(D.isDirectionalLight){let j=e.get(D);if(j.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let te=D.shadow,W=t.get(D);W.shadowIntensity=te.intensity,W.shadowBias=te.bias,W.shadowNormalBias=te.normalBias,W.shadowRadius=te.radius,W.shadowMapSize=te.mapSize,i.directionalShadow[f]=W,i.directionalShadowMap[f]=K,i.directionalShadowMatrix[f]=D.shadow.matrix,S++}i.directional[f]=j,f++}else if(D.isSpotLight){let j=e.get(D);j.position.setFromMatrixPosition(D.matrixWorld),j.color.copy(z).multiplyScalar(H),j.distance=$,j.coneCos=Math.cos(D.angle),j.penumbraCos=Math.cos(D.angle*(1-D.penumbra)),j.decay=D.decay,i.spot[v]=j;let te=D.shadow;if(D.map&&(i.spotLightMap[F]=D.map,F++,te.updateMatrices(D),D.castShadow&&T++),i.spotLightMatrix[v]=te.matrix,D.castShadow){let W=t.get(D);W.shadowIntensity=te.intensity,W.shadowBias=te.bias,W.shadowNormalBias=te.normalBias,W.shadowRadius=te.radius,W.shadowMapSize=te.mapSize,i.spotShadow[v]=W,i.spotShadowMap[v]=K,b++}v++}else if(D.isRectAreaLight){let j=e.get(D);j.color.copy(z).multiplyScalar(H),j.halfWidth.set(D.width*.5,0,0),j.halfHeight.set(0,D.height*.5,0),i.rectArea[m]=j,m++}else if(D.isPointLight){let j=e.get(D);if(j.color.copy(D.color).multiplyScalar(D.intensity),j.distance=D.distance,j.decay=D.decay,D.castShadow){let te=D.shadow,W=t.get(D);W.shadowIntensity=te.intensity,W.shadowBias=te.bias,W.shadowNormalBias=te.normalBias,W.shadowRadius=te.radius,W.shadowMapSize=te.mapSize,W.shadowCameraNear=te.camera.near,W.shadowCameraFar=te.camera.far,i.pointShadow[g]=W,i.pointShadowMap[g]=K,i.pointShadowMatrix[g]=D.shadow.matrix,E++}i.point[g]=j,g++}else if(D.isHemisphereLight){let j=e.get(D);j.skyColor.copy(D.color).multiplyScalar(H),j.groundColor.copy(D.groundColor).multiplyScalar(H),i.hemi[p]=j,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=oe.LTC_FLOAT_1,i.rectAreaLTC2=oe.LTC_FLOAT_2):(i.rectAreaLTC1=oe.LTC_HALF_1,i.rectAreaLTC2=oe.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=h;let A=i.hash;(A.directionalLength!==f||A.pointLength!==g||A.spotLength!==v||A.rectAreaLength!==m||A.hemiLength!==p||A.numDirectionalShadows!==S||A.numPointShadows!==E||A.numSpotShadows!==b||A.numSpotMaps!==F||A.numLightProbes!==C)&&(i.directional.length=f,i.spot.length=v,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=S,i.directionalShadowMap.length=S,i.pointShadow.length=E,i.pointShadowMap.length=E,i.spotShadow.length=b,i.spotShadowMap.length=b,i.directionalShadowMatrix.length=S,i.pointShadowMatrix.length=E,i.spotLightMatrix.length=b+F-T,i.spotLightMap.length=F,i.numSpotLightShadowsWithMaps=T,i.numLightProbes=C,A.directionalLength=f,A.pointLength=g,A.spotLength=v,A.rectAreaLength=m,A.hemiLength=p,A.numDirectionalShadows=S,A.numPointShadows=E,A.numSpotShadows=b,A.numSpotMaps=F,A.numLightProbes=C,i.version=hN++)}function l(c,u){let d=0,h=0,f=0,g=0,v=0,m=u.matrixWorldInverse;for(let p=0,S=c.length;p<S;p++){let E=c[p];if(E.isDirectionalLight){let b=i.directional[d];b.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),b.direction.sub(r),b.direction.transformDirection(m),d++}else if(E.isSpotLight){let b=i.spot[f];b.position.setFromMatrixPosition(E.matrixWorld),b.position.applyMatrix4(m),b.direction.setFromMatrixPosition(E.matrixWorld),r.setFromMatrixPosition(E.target.matrixWorld),b.direction.sub(r),b.direction.transformDirection(m),f++}else if(E.isRectAreaLight){let b=i.rectArea[g];b.position.setFromMatrixPosition(E.matrixWorld),b.position.applyMatrix4(m),o.identity(),s.copy(E.matrixWorld),s.premultiply(m),o.extractRotation(s),b.halfWidth.set(E.width*.5,0,0),b.halfHeight.set(0,E.height*.5,0),b.halfWidth.applyMatrix4(o),b.halfHeight.applyMatrix4(o),g++}else if(E.isPointLight){let b=i.point[h];b.position.setFromMatrixPosition(E.matrixWorld),b.position.applyMatrix4(m),h++}else if(E.isHemisphereLight){let b=i.hemi[v];b.direction.setFromMatrixPosition(E.matrixWorld),b.direction.transformDirection(m),v++}}}return{setup:a,setupView:l,state:i}}function K0(n){let e=new mN(n),t=[],i=[];function r(u){c.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function l(u){e.setupView(t,u)}let c={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:c,setupLights:a,setupLightsView:l,pushLight:s,pushShadow:o}}function gN(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new K0(n),e.set(r,[a])):s>=o.length?(a=new K0(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var Qp=class extends gr{static get type(){return"MeshDepthMaterial"}constructor(e){super(),this.isMeshDepthMaterial=!0,this.depthPacking=ED,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},em=class extends gr{static get type(){return"MeshDistanceMaterial"}constructor(e){super(),this.isMeshDistanceMaterial=!0,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}},vN=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,yN=`uniform sampler2D shadow_pass;
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
}`;function _N(n,e,t){let i=new ba,r=new Qe,s=new Qe,o=new mt,a=new Qp({depthPacking:SD}),l=new em,c={},u=t.maxTextureSize,d={[hr]:bn,[bn]:hr,[Ui]:Ui},h=new di({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new Qe},radius:{value:4}},vertexShader:vN,fragmentShader:yN}),f=h.clone();f.defines.HORIZONTAL_PASS=1;let g=new Cn;g.setAttribute("position",new ln(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let v=new Xt(g,h),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=ox;let p=this.type;this.render=function(T,C,A){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||T.length===0)return;let M=n.getRenderTarget(),_=n.getActiveCubeFace(),D=n.getActiveMipmapLevel(),z=n.state;z.setBlending(dr),z.buffers.color.setClear(1,1,1,1),z.buffers.depth.setTest(!0),z.setScissorTest(!1);let H=p!==Li&&this.type===Li,$=p===Li&&this.type!==Li;for(let K=0,j=T.length;K<j;K++){let te=T[K],W=te.shadow;if(W===void 0){console.warn("THREE.WebGLShadowMap:",te,"has no shadow.");continue}if(W.autoUpdate===!1&&W.needsUpdate===!1)continue;r.copy(W.mapSize);let le=W.getFrameExtents();if(r.multiply(le),s.copy(W.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/le.x),r.x=s.x*le.x,W.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/le.y),r.y=s.y*le.y,W.mapSize.y=s.y)),W.map===null||H===!0||$===!0){let Ce=this.type!==Li?{minFilter:Qn,magFilter:Qn}:{};W.map!==null&&W.map.dispose(),W.map=new Wi(r.x,r.y,Ce),W.map.texture.name=te.name+".shadowMap",W.camera.updateProjectionMatrix()}n.setRenderTarget(W.map),n.clear();let pe=W.getViewportCount();for(let Ce=0;Ce<pe;Ce++){let qe=W.getViewport(Ce);o.set(s.x*qe.x,s.y*qe.y,s.x*qe.z,s.y*qe.w),z.viewport(o),W.updateMatrices(te,Ce),i=W.getFrustum(),b(C,A,W.camera,te,this.type)}W.isPointLightShadow!==!0&&this.type===Li&&S(W,A),W.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(M,_,D)};function S(T,C){let A=e.update(v);h.defines.VSM_SAMPLES!==T.blurSamples&&(h.defines.VSM_SAMPLES=T.blurSamples,f.defines.VSM_SAMPLES=T.blurSamples,h.needsUpdate=!0,f.needsUpdate=!0),T.mapPass===null&&(T.mapPass=new Wi(r.x,r.y)),h.uniforms.shadow_pass.value=T.map.texture,h.uniforms.resolution.value=T.mapSize,h.uniforms.radius.value=T.radius,n.setRenderTarget(T.mapPass),n.clear(),n.renderBufferDirect(C,null,A,h,v,null),f.uniforms.shadow_pass.value=T.mapPass.texture,f.uniforms.resolution.value=T.mapSize,f.uniforms.radius.value=T.radius,n.setRenderTarget(T.map),n.clear(),n.renderBufferDirect(C,null,A,f,v,null)}function E(T,C,A,M){let _=null,D=A.isPointLight===!0?T.customDistanceMaterial:T.customDepthMaterial;if(D!==void 0)_=D;else if(_=A.isPointLight===!0?l:a,n.localClippingEnabled&&C.clipShadows===!0&&Array.isArray(C.clippingPlanes)&&C.clippingPlanes.length!==0||C.displacementMap&&C.displacementScale!==0||C.alphaMap&&C.alphaTest>0||C.map&&C.alphaTest>0){let z=_.uuid,H=C.uuid,$=c[z];$===void 0&&($={},c[z]=$);let K=$[H];K===void 0&&(K=_.clone(),$[H]=K,C.addEventListener("dispose",F)),_=K}if(_.visible=C.visible,_.wireframe=C.wireframe,M===Li?_.side=C.shadowSide!==null?C.shadowSide:C.side:_.side=C.shadowSide!==null?C.shadowSide:d[C.side],_.alphaMap=C.alphaMap,_.alphaTest=C.alphaTest,_.map=C.map,_.clipShadows=C.clipShadows,_.clippingPlanes=C.clippingPlanes,_.clipIntersection=C.clipIntersection,_.displacementMap=C.displacementMap,_.displacementScale=C.displacementScale,_.displacementBias=C.displacementBias,_.wireframeLinewidth=C.wireframeLinewidth,_.linewidth=C.linewidth,A.isPointLight===!0&&_.isMeshDistanceMaterial===!0){let z=n.properties.get(_);z.light=A}return _}function b(T,C,A,M,_){if(T.visible===!1)return;if(T.layers.test(C.layers)&&(T.isMesh||T.isLine||T.isPoints)&&(T.castShadow||T.receiveShadow&&_===Li)&&(!T.frustumCulled||i.intersectsObject(T))){T.modelViewMatrix.multiplyMatrices(A.matrixWorldInverse,T.matrixWorld);let H=e.update(T),$=T.material;if(Array.isArray($)){let K=H.groups;for(let j=0,te=K.length;j<te;j++){let W=K[j],le=$[W.materialIndex];if(le&&le.visible){let pe=E(T,le,M,_);T.onBeforeShadow(n,T,C,A,H,pe,W),n.renderBufferDirect(A,null,H,pe,T,W),T.onAfterShadow(n,T,C,A,H,pe,W)}}}else if($.visible){let K=E(T,$,M,_);T.onBeforeShadow(n,T,C,A,H,K,null),n.renderBufferDirect(A,null,H,K,T,null),T.onAfterShadow(n,T,C,A,H,K,null)}}let z=T.children;for(let H=0,$=z.length;H<$;H++)b(z[H],C,A,M,_)}function F(T){T.target.removeEventListener("dispose",F);for(let A in c){let M=c[A],_=T.target.uuid;_ in M&&(M[_].dispose(),delete M[_])}}}var xN={[np]:ip,[rp]:ap,[sp]:lp,[oo]:op,[ip]:np,[ap]:rp,[lp]:sp,[op]:oo};function bN(n,e){function t(){let I=!1,ae=new mt,G=null,Z=new mt(0,0,0,0);return{setMask:function(he){G!==he&&!I&&(n.colorMask(he,he,he,he),G=he)},setLocked:function(he){I=he},setClear:function(he,de,Ue,It,Zt){Zt===!0&&(he*=It,de*=It,Ue*=It),ae.set(he,de,Ue,It),Z.equals(ae)===!1&&(n.clearColor(he,de,Ue,It),Z.copy(ae))},reset:function(){I=!1,G=null,Z.set(-1,0,0,0)}}}function i(){let I=!1,ae=!1,G=null,Z=null,he=null;return{setReversed:function(de){if(ae!==de){let Ue=e.get("EXT_clip_control");ae?Ue.clipControlEXT(Ue.LOWER_LEFT_EXT,Ue.ZERO_TO_ONE_EXT):Ue.clipControlEXT(Ue.LOWER_LEFT_EXT,Ue.NEGATIVE_ONE_TO_ONE_EXT);let It=he;he=null,this.setClear(It)}ae=de},getReversed:function(){return ae},setTest:function(de){de?ce(n.DEPTH_TEST):Re(n.DEPTH_TEST)},setMask:function(de){G!==de&&!I&&(n.depthMask(de),G=de)},setFunc:function(de){if(ae&&(de=xN[de]),Z!==de){switch(de){case np:n.depthFunc(n.NEVER);break;case ip:n.depthFunc(n.ALWAYS);break;case rp:n.depthFunc(n.LESS);break;case oo:n.depthFunc(n.LEQUAL);break;case sp:n.depthFunc(n.EQUAL);break;case op:n.depthFunc(n.GEQUAL);break;case ap:n.depthFunc(n.GREATER);break;case lp:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}Z=de}},setLocked:function(de){I=de},setClear:function(de){he!==de&&(ae&&(de=1-de),n.clearDepth(de),he=de)},reset:function(){I=!1,G=null,Z=null,he=null,ae=!1}}}function r(){let I=!1,ae=null,G=null,Z=null,he=null,de=null,Ue=null,It=null,Zt=null;return{setTest:function(ut){I||(ut?ce(n.STENCIL_TEST):Re(n.STENCIL_TEST))},setMask:function(ut){ae!==ut&&!I&&(n.stencilMask(ut),ae=ut)},setFunc:function(ut,Bn,hi){(G!==ut||Z!==Bn||he!==hi)&&(n.stencilFunc(ut,Bn,hi),G=ut,Z=Bn,he=hi)},setOp:function(ut,Bn,hi){(de!==ut||Ue!==Bn||It!==hi)&&(n.stencilOp(ut,Bn,hi),de=ut,Ue=Bn,It=hi)},setLocked:function(ut){I=ut},setClear:function(ut){Zt!==ut&&(n.clearStencil(ut),Zt=ut)},reset:function(){I=!1,ae=null,G=null,Z=null,he=null,de=null,Ue=null,It=null,Zt=null}}}let s=new t,o=new i,a=new r,l=new WeakMap,c=new WeakMap,u={},d={},h=new WeakMap,f=[],g=null,v=!1,m=null,p=null,S=null,E=null,b=null,F=null,T=null,C=new $e(0,0,0),A=0,M=!1,_=null,D=null,z=null,H=null,$=null,K=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),j=!1,te=0,W=n.getParameter(n.VERSION);W.indexOf("WebGL")!==-1?(te=parseFloat(/^WebGL (\d)/.exec(W)[1]),j=te>=1):W.indexOf("OpenGL ES")!==-1&&(te=parseFloat(/^OpenGL ES (\d)/.exec(W)[1]),j=te>=2);let le=null,pe={},Ce=n.getParameter(n.SCISSOR_BOX),qe=n.getParameter(n.VIEWPORT),gt=new mt().fromArray(Ce),X=new mt().fromArray(qe);function re(I,ae,G,Z){let he=new Uint8Array(4),de=n.createTexture();n.bindTexture(I,de),n.texParameteri(I,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(I,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Ue=0;Ue<G;Ue++)I===n.TEXTURE_3D||I===n.TEXTURE_2D_ARRAY?n.texImage3D(ae,0,n.RGBA,1,1,Z,0,n.RGBA,n.UNSIGNED_BYTE,he):n.texImage2D(ae+Ue,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,he);return de}let be={};be[n.TEXTURE_2D]=re(n.TEXTURE_2D,n.TEXTURE_2D,1),be[n.TEXTURE_CUBE_MAP]=re(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),be[n.TEXTURE_2D_ARRAY]=re(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),be[n.TEXTURE_3D]=re(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ce(n.DEPTH_TEST),o.setFunc(oo),et(!1),tt(Q_),ce(n.CULL_FACE),P(dr);function ce(I){u[I]!==!0&&(n.enable(I),u[I]=!0)}function Re(I){u[I]!==!1&&(n.disable(I),u[I]=!1)}function ke(I,ae){return d[I]!==ae?(n.bindFramebuffer(I,ae),d[I]=ae,I===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=ae),I===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=ae),!0):!1}function Xe(I,ae){let G=f,Z=!1;if(I){G=h.get(ae),G===void 0&&(G=[],h.set(ae,G));let he=I.textures;if(G.length!==he.length||G[0]!==n.COLOR_ATTACHMENT0){for(let de=0,Ue=he.length;de<Ue;de++)G[de]=n.COLOR_ATTACHMENT0+de;G.length=he.length,Z=!0}}else G[0]!==n.BACK&&(G[0]=n.BACK,Z=!0);Z&&n.drawBuffers(G)}function wt(I){return g!==I?(n.useProgram(I),g=I,!0):!1}let rt={[ns]:n.FUNC_ADD,[ZT]:n.FUNC_SUBTRACT,[JT]:n.FUNC_REVERSE_SUBTRACT};rt[KT]=n.MIN,rt[QT]=n.MAX;let Ft={[eD]:n.ZERO,[tD]:n.ONE,[nD]:n.SRC_COLOR,[ep]:n.SRC_ALPHA,[lD]:n.SRC_ALPHA_SATURATE,[oD]:n.DST_COLOR,[rD]:n.DST_ALPHA,[iD]:n.ONE_MINUS_SRC_COLOR,[tp]:n.ONE_MINUS_SRC_ALPHA,[aD]:n.ONE_MINUS_DST_COLOR,[sD]:n.ONE_MINUS_DST_ALPHA,[cD]:n.CONSTANT_COLOR,[uD]:n.ONE_MINUS_CONSTANT_COLOR,[dD]:n.CONSTANT_ALPHA,[fD]:n.ONE_MINUS_CONSTANT_ALPHA};function P(I,ae,G,Z,he,de,Ue,It,Zt,ut){if(I===dr){v===!0&&(Re(n.BLEND),v=!1);return}if(v===!1&&(ce(n.BLEND),v=!0),I!==YT){if(I!==m||ut!==M){if((p!==ns||b!==ns)&&(n.blendEquation(n.FUNC_ADD),p=ns,b=ns),ut)switch(I){case no:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case e0:n.blendFunc(n.ONE,n.ONE);break;case t0:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case n0:n.blendFuncSeparate(n.ZERO,n.SRC_COLOR,n.ZERO,n.SRC_ALPHA);break;default:console.error("THREE.WebGLState: Invalid blending: ",I);break}else switch(I){case no:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case e0:n.blendFunc(n.SRC_ALPHA,n.ONE);break;case t0:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case n0:n.blendFunc(n.ZERO,n.SRC_COLOR);break;default:console.error("THREE.WebGLState: Invalid blending: ",I);break}S=null,E=null,F=null,T=null,C.set(0,0,0),A=0,m=I,M=ut}return}he=he||ae,de=de||G,Ue=Ue||Z,(ae!==p||he!==b)&&(n.blendEquationSeparate(rt[ae],rt[he]),p=ae,b=he),(G!==S||Z!==E||de!==F||Ue!==T)&&(n.blendFuncSeparate(Ft[G],Ft[Z],Ft[de],Ft[Ue]),S=G,E=Z,F=de,T=Ue),(It.equals(C)===!1||Zt!==A)&&(n.blendColor(It.r,It.g,It.b,Zt),C.copy(It),A=Zt),m=I,M=!1}function Tn(I,ae){I.side===Ui?Re(n.CULL_FACE):ce(n.CULL_FACE);let G=I.side===bn;ae&&(G=!G),et(G),I.blending===no&&I.transparent===!1?P(dr):P(I.blending,I.blendEquation,I.blendSrc,I.blendDst,I.blendEquationAlpha,I.blendSrcAlpha,I.blendDstAlpha,I.blendColor,I.blendAlpha,I.premultipliedAlpha),o.setFunc(I.depthFunc),o.setTest(I.depthTest),o.setMask(I.depthWrite),s.setMask(I.colorWrite);let Z=I.stencilWrite;a.setTest(Z),Z&&(a.setMask(I.stencilWriteMask),a.setFunc(I.stencilFunc,I.stencilRef,I.stencilFuncMask),a.setOp(I.stencilFail,I.stencilZFail,I.stencilZPass)),xt(I.polygonOffset,I.polygonOffsetFactor,I.polygonOffsetUnits),I.alphaToCoverage===!0?ce(n.SAMPLE_ALPHA_TO_COVERAGE):Re(n.SAMPLE_ALPHA_TO_COVERAGE)}function et(I){_!==I&&(I?n.frontFace(n.CW):n.frontFace(n.CCW),_=I)}function tt(I){I!==$T?(ce(n.CULL_FACE),I!==D&&(I===Q_?n.cullFace(n.BACK):I===qT?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Re(n.CULL_FACE),D=I}function Ae(I){I!==z&&(j&&n.lineWidth(I),z=I)}function xt(I,ae,G){I?(ce(n.POLYGON_OFFSET_FILL),(H!==ae||$!==G)&&(n.polygonOffset(ae,G),H=ae,$=G)):Re(n.POLYGON_OFFSET_FILL)}function Te(I){I?ce(n.SCISSOR_TEST):Re(n.SCISSOR_TEST)}function w(I){I===void 0&&(I=n.TEXTURE0+K-1),le!==I&&(n.activeTexture(I),le=I)}function y(I,ae,G){G===void 0&&(le===null?G=n.TEXTURE0+K-1:G=le);let Z=pe[G];Z===void 0&&(Z={type:void 0,texture:void 0},pe[G]=Z),(Z.type!==I||Z.texture!==ae)&&(le!==G&&(n.activeTexture(G),le=G),n.bindTexture(I,ae||be[I]),Z.type=I,Z.texture=ae)}function k(){let I=pe[le];I!==void 0&&I.type!==void 0&&(n.bindTexture(I.type,null),I.type=void 0,I.texture=void 0)}function Y(){try{n.compressedTexImage2D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function ee(){try{n.compressedTexImage3D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function q(){try{n.texSubImage2D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function Me(){try{n.texSubImage3D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function ue(){try{n.compressedTexSubImage2D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function me(){try{n.compressedTexSubImage3D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function st(){try{n.texStorage2D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function ne(){try{n.texStorage3D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function ge(){try{n.texImage2D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function Ie(){try{n.texImage3D.apply(n,arguments)}catch(I){console.error("THREE.WebGLState:",I)}}function Pe(I){gt.equals(I)===!1&&(n.scissor(I.x,I.y,I.z,I.w),gt.copy(I))}function ve(I){X.equals(I)===!1&&(n.viewport(I.x,I.y,I.z,I.w),X.copy(I))}function nt(I,ae){let G=c.get(ae);G===void 0&&(G=new WeakMap,c.set(ae,G));let Z=G.get(I);Z===void 0&&(Z=n.getUniformBlockIndex(ae,I.name),G.set(I,Z))}function He(I,ae){let Z=c.get(ae).get(I);l.get(ae)!==Z&&(n.uniformBlockBinding(ae,Z,I.__bindingPointIndex),l.set(ae,Z))}function vt(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},le=null,pe={},d={},h=new WeakMap,f=[],g=null,v=!1,m=null,p=null,S=null,E=null,b=null,F=null,T=null,C=new $e(0,0,0),A=0,M=!1,_=null,D=null,z=null,H=null,$=null,gt.set(0,0,n.canvas.width,n.canvas.height),X.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ce,disable:Re,bindFramebuffer:ke,drawBuffers:Xe,useProgram:wt,setBlending:P,setMaterial:Tn,setFlipSided:et,setCullFace:tt,setLineWidth:Ae,setPolygonOffset:xt,setScissorTest:Te,activeTexture:w,bindTexture:y,unbindTexture:k,compressedTexImage2D:Y,compressedTexImage3D:ee,texImage2D:ge,texImage3D:Ie,updateUBOMapping:nt,uniformBlockBinding:He,texStorage2D:st,texStorage3D:ne,texSubImage2D:q,texSubImage3D:Me,compressedTexSubImage2D:ue,compressedTexSubImage3D:me,scissor:Pe,viewport:ve,reset:vt}}function Q0(n,e,t,i){let r=MN(i);switch(t){case dx:return n*e;case hx:return n*e;case px:return n*e*2;case mx:return n*e/r.components*r.byteLength;case _m:return n*e/r.components*r.byteLength;case gx:return n*e*2/r.components*r.byteLength;case xm:return n*e*2/r.components*r.byteLength;case fx:return n*e*3/r.components*r.byteLength;case Kn:return n*e*4/r.components*r.byteLength;case bm:return n*e*4/r.components*r.byteLength;case kc:case Uc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Vc:case Bc:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case pp:case gp:return Math.max(n,16)*Math.max(e,8)/4;case hp:case mp:return Math.max(n,8)*Math.max(e,8)/2;case vp:case yp:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case _p:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case xp:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case bp:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case Mp:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Ep:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Sp:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case wp:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Cp:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Tp:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Dp:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Ap:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Ip:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Rp:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Np:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Pp:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Hc:case Op:case Fp:return Math.ceil(n/4)*Math.ceil(e/4)*16;case vx:case Lp:return Math.ceil(n/4)*Math.ceil(e/4)*8;case kp:case Up:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function MN(n){switch(n){case Gi:case lx:return{byteLength:1,components:1};case _a:case cx:case Sa:return{byteLength:2,components:1};case vm:case ym:return{byteLength:2,components:4};case as:case gm:case Vi:return{byteLength:4,components:1};case ux:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}function EN(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,l=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),c=new Qe,u=new WeakMap,d,h=new WeakMap,f=!1;try{f=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function g(w,y){return f?new OffscreenCanvas(w,y):Wc("canvas")}function v(w,y,k){let Y=1,ee=Te(w);if((ee.width>k||ee.height>k)&&(Y=k/Math.max(ee.width,ee.height)),Y<1)if(typeof HTMLImageElement<"u"&&w instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&w instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&w instanceof ImageBitmap||typeof VideoFrame<"u"&&w instanceof VideoFrame){let q=Math.floor(Y*ee.width),Me=Math.floor(Y*ee.height);d===void 0&&(d=g(q,Me));let ue=y?g(q,Me):d;return ue.width=q,ue.height=Me,ue.getContext("2d").drawImage(w,0,0,q,Me),console.warn("THREE.WebGLRenderer: Texture has been resized from ("+ee.width+"x"+ee.height+") to ("+q+"x"+Me+")."),ue}else return"data"in w&&console.warn("THREE.WebGLRenderer: Image in DataTexture is too big ("+ee.width+"x"+ee.height+")."),w;return w}function m(w){return w.generateMipmaps}function p(w){n.generateMipmap(w)}function S(w){return w.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:w.isWebGL3DRenderTarget?n.TEXTURE_3D:w.isWebGLArrayRenderTarget||w.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function E(w,y,k,Y,ee=!1){if(w!==null){if(n[w]!==void 0)return n[w];console.warn("THREE.WebGLRenderer: Attempt to use non-existing WebGL internal format '"+w+"'")}let q=y;if(y===n.RED&&(k===n.FLOAT&&(q=n.R32F),k===n.HALF_FLOAT&&(q=n.R16F),k===n.UNSIGNED_BYTE&&(q=n.R8)),y===n.RED_INTEGER&&(k===n.UNSIGNED_BYTE&&(q=n.R8UI),k===n.UNSIGNED_SHORT&&(q=n.R16UI),k===n.UNSIGNED_INT&&(q=n.R32UI),k===n.BYTE&&(q=n.R8I),k===n.SHORT&&(q=n.R16I),k===n.INT&&(q=n.R32I)),y===n.RG&&(k===n.FLOAT&&(q=n.RG32F),k===n.HALF_FLOAT&&(q=n.RG16F),k===n.UNSIGNED_BYTE&&(q=n.RG8)),y===n.RG_INTEGER&&(k===n.UNSIGNED_BYTE&&(q=n.RG8UI),k===n.UNSIGNED_SHORT&&(q=n.RG16UI),k===n.UNSIGNED_INT&&(q=n.RG32UI),k===n.BYTE&&(q=n.RG8I),k===n.SHORT&&(q=n.RG16I),k===n.INT&&(q=n.RG32I)),y===n.RGB_INTEGER&&(k===n.UNSIGNED_BYTE&&(q=n.RGB8UI),k===n.UNSIGNED_SHORT&&(q=n.RGB16UI),k===n.UNSIGNED_INT&&(q=n.RGB32UI),k===n.BYTE&&(q=n.RGB8I),k===n.SHORT&&(q=n.RGB16I),k===n.INT&&(q=n.RGB32I)),y===n.RGBA_INTEGER&&(k===n.UNSIGNED_BYTE&&(q=n.RGBA8UI),k===n.UNSIGNED_SHORT&&(q=n.RGBA16UI),k===n.UNSIGNED_INT&&(q=n.RGBA32UI),k===n.BYTE&&(q=n.RGBA8I),k===n.SHORT&&(q=n.RGBA16I),k===n.INT&&(q=n.RGBA32I)),y===n.RGB&&k===n.UNSIGNED_INT_5_9_9_9_REV&&(q=n.RGB9_E5),y===n.RGBA){let Me=ee?pu:ot.getTransfer(Y);k===n.FLOAT&&(q=n.RGBA32F),k===n.HALF_FLOAT&&(q=n.RGBA16F),k===n.UNSIGNED_BYTE&&(q=Me===pt?n.SRGB8_ALPHA8:n.RGBA8),k===n.UNSIGNED_SHORT_4_4_4_4&&(q=n.RGBA4),k===n.UNSIGNED_SHORT_5_5_5_1&&(q=n.RGB5_A1)}return(q===n.R16F||q===n.R32F||q===n.RG16F||q===n.RG32F||q===n.RGBA16F||q===n.RGBA32F)&&e.get("EXT_color_buffer_float"),q}function b(w,y){let k;return w?y===null||y===as||y===co?k=n.DEPTH24_STENCIL8:y===Vi?k=n.DEPTH32F_STENCIL8:y===_a&&(k=n.DEPTH24_STENCIL8,console.warn("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):y===null||y===as||y===co?k=n.DEPTH_COMPONENT24:y===Vi?k=n.DEPTH_COMPONENT32F:y===_a&&(k=n.DEPTH_COMPONENT16),k}function F(w,y){return m(w)===!0||w.isFramebufferTexture&&w.minFilter!==Qn&&w.minFilter!==ui?Math.log2(Math.max(y.width,y.height))+1:w.mipmaps!==void 0&&w.mipmaps.length>0?w.mipmaps.length:w.isCompressedTexture&&Array.isArray(w.image)?y.mipmaps.length:1}function T(w){let y=w.target;y.removeEventListener("dispose",T),A(y),y.isVideoTexture&&u.delete(y)}function C(w){let y=w.target;y.removeEventListener("dispose",C),_(y)}function A(w){let y=i.get(w);if(y.__webglInit===void 0)return;let k=w.source,Y=h.get(k);if(Y){let ee=Y[y.__cacheKey];ee.usedTimes--,ee.usedTimes===0&&M(w),Object.keys(Y).length===0&&h.delete(k)}i.remove(w)}function M(w){let y=i.get(w);n.deleteTexture(y.__webglTexture);let k=w.source,Y=h.get(k);delete Y[y.__cacheKey],o.memory.textures--}function _(w){let y=i.get(w);if(w.depthTexture&&(w.depthTexture.dispose(),i.remove(w.depthTexture)),w.isWebGLCubeRenderTarget)for(let Y=0;Y<6;Y++){if(Array.isArray(y.__webglFramebuffer[Y]))for(let ee=0;ee<y.__webglFramebuffer[Y].length;ee++)n.deleteFramebuffer(y.__webglFramebuffer[Y][ee]);else n.deleteFramebuffer(y.__webglFramebuffer[Y]);y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer[Y])}else{if(Array.isArray(y.__webglFramebuffer))for(let Y=0;Y<y.__webglFramebuffer.length;Y++)n.deleteFramebuffer(y.__webglFramebuffer[Y]);else n.deleteFramebuffer(y.__webglFramebuffer);if(y.__webglDepthbuffer&&n.deleteRenderbuffer(y.__webglDepthbuffer),y.__webglMultisampledFramebuffer&&n.deleteFramebuffer(y.__webglMultisampledFramebuffer),y.__webglColorRenderbuffer)for(let Y=0;Y<y.__webglColorRenderbuffer.length;Y++)y.__webglColorRenderbuffer[Y]&&n.deleteRenderbuffer(y.__webglColorRenderbuffer[Y]);y.__webglDepthRenderbuffer&&n.deleteRenderbuffer(y.__webglDepthRenderbuffer)}let k=w.textures;for(let Y=0,ee=k.length;Y<ee;Y++){let q=i.get(k[Y]);q.__webglTexture&&(n.deleteTexture(q.__webglTexture),o.memory.textures--),i.remove(k[Y])}i.remove(w)}let D=0;function z(){D=0}function H(){let w=D;return w>=r.maxTextures&&console.warn("THREE.WebGLTextures: Trying to use "+w+" texture units while this GPU supports only "+r.maxTextures),D+=1,w}function $(w){let y=[];return y.push(w.wrapS),y.push(w.wrapT),y.push(w.wrapR||0),y.push(w.magFilter),y.push(w.minFilter),y.push(w.anisotropy),y.push(w.internalFormat),y.push(w.format),y.push(w.type),y.push(w.generateMipmaps),y.push(w.premultiplyAlpha),y.push(w.flipY),y.push(w.unpackAlignment),y.push(w.colorSpace),y.join()}function K(w,y){let k=i.get(w);if(w.isVideoTexture&&Ae(w),w.isRenderTargetTexture===!1&&w.version>0&&k.__version!==w.version){let Y=w.image;if(Y===null)console.warn("THREE.WebGLRenderer: Texture marked for update but no image data found.");else if(Y.complete===!1)console.warn("THREE.WebGLRenderer: Texture marked for update but image is incomplete");else{X(k,w,y);return}}t.bindTexture(n.TEXTURE_2D,k.__webglTexture,n.TEXTURE0+y)}function j(w,y){let k=i.get(w);if(w.version>0&&k.__version!==w.version){X(k,w,y);return}t.bindTexture(n.TEXTURE_2D_ARRAY,k.__webglTexture,n.TEXTURE0+y)}function te(w,y){let k=i.get(w);if(w.version>0&&k.__version!==w.version){X(k,w,y);return}t.bindTexture(n.TEXTURE_3D,k.__webglTexture,n.TEXTURE0+y)}function W(w,y){let k=i.get(w);if(w.version>0&&k.__version!==w.version){re(k,w,y);return}t.bindTexture(n.TEXTURE_CUBE_MAP,k.__webglTexture,n.TEXTURE0+y)}let le={[dp]:n.REPEAT,[ss]:n.CLAMP_TO_EDGE,[fp]:n.MIRRORED_REPEAT},pe={[Qn]:n.NEAREST,[MD]:n.NEAREST_MIPMAP_NEAREST,[hc]:n.NEAREST_MIPMAP_LINEAR,[ui]:n.LINEAR,[Mh]:n.LINEAR_MIPMAP_NEAREST,[os]:n.LINEAR_MIPMAP_LINEAR},Ce={[CD]:n.NEVER,[ND]:n.ALWAYS,[TD]:n.LESS,[_x]:n.LEQUAL,[DD]:n.EQUAL,[RD]:n.GEQUAL,[AD]:n.GREATER,[ID]:n.NOTEQUAL};function qe(w,y){if(y.type===Vi&&e.has("OES_texture_float_linear")===!1&&(y.magFilter===ui||y.magFilter===Mh||y.magFilter===hc||y.magFilter===os||y.minFilter===ui||y.minFilter===Mh||y.minFilter===hc||y.minFilter===os)&&console.warn("THREE.WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(w,n.TEXTURE_WRAP_S,le[y.wrapS]),n.texParameteri(w,n.TEXTURE_WRAP_T,le[y.wrapT]),(w===n.TEXTURE_3D||w===n.TEXTURE_2D_ARRAY)&&n.texParameteri(w,n.TEXTURE_WRAP_R,le[y.wrapR]),n.texParameteri(w,n.TEXTURE_MAG_FILTER,pe[y.magFilter]),n.texParameteri(w,n.TEXTURE_MIN_FILTER,pe[y.minFilter]),y.compareFunction&&(n.texParameteri(w,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(w,n.TEXTURE_COMPARE_FUNC,Ce[y.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(y.magFilter===Qn||y.minFilter!==hc&&y.minFilter!==os||y.type===Vi&&e.has("OES_texture_float_linear")===!1)return;if(y.anisotropy>1||i.get(y).__currentAnisotropy){let k=e.get("EXT_texture_filter_anisotropic");n.texParameterf(w,k.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(y.anisotropy,r.getMaxAnisotropy())),i.get(y).__currentAnisotropy=y.anisotropy}}}function gt(w,y){let k=!1;w.__webglInit===void 0&&(w.__webglInit=!0,y.addEventListener("dispose",T));let Y=y.source,ee=h.get(Y);ee===void 0&&(ee={},h.set(Y,ee));let q=$(y);if(q!==w.__cacheKey){ee[q]===void 0&&(ee[q]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,k=!0),ee[q].usedTimes++;let Me=ee[w.__cacheKey];Me!==void 0&&(ee[w.__cacheKey].usedTimes--,Me.usedTimes===0&&M(y)),w.__cacheKey=q,w.__webglTexture=ee[q].texture}return k}function X(w,y,k){let Y=n.TEXTURE_2D;(y.isDataArrayTexture||y.isCompressedArrayTexture)&&(Y=n.TEXTURE_2D_ARRAY),y.isData3DTexture&&(Y=n.TEXTURE_3D);let ee=gt(w,y),q=y.source;t.bindTexture(Y,w.__webglTexture,n.TEXTURE0+k);let Me=i.get(q);if(q.version!==Me.__version||ee===!0){t.activeTexture(n.TEXTURE0+k);let ue=ot.getPrimaries(ot.workingColorSpace),me=y.colorSpace===ur?null:ot.getPrimaries(y.colorSpace),st=y.colorSpace===ur||ue===me?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,st);let ne=v(y.image,!1,r.maxTextureSize);ne=xt(y,ne);let ge=s.convert(y.format,y.colorSpace),Ie=s.convert(y.type),Pe=E(y.internalFormat,ge,Ie,y.colorSpace,y.isVideoTexture);qe(Y,y);let ve,nt=y.mipmaps,He=y.isVideoTexture!==!0,vt=Me.__version===void 0||ee===!0,I=q.dataReady,ae=F(y,ne);if(y.isDepthTexture)Pe=b(y.format===uo,y.type),vt&&(He?t.texStorage2D(n.TEXTURE_2D,1,Pe,ne.width,ne.height):t.texImage2D(n.TEXTURE_2D,0,Pe,ne.width,ne.height,0,ge,Ie,null));else if(y.isDataTexture)if(nt.length>0){He&&vt&&t.texStorage2D(n.TEXTURE_2D,ae,Pe,nt[0].width,nt[0].height);for(let G=0,Z=nt.length;G<Z;G++)ve=nt[G],He?I&&t.texSubImage2D(n.TEXTURE_2D,G,0,0,ve.width,ve.height,ge,Ie,ve.data):t.texImage2D(n.TEXTURE_2D,G,Pe,ve.width,ve.height,0,ge,Ie,ve.data);y.generateMipmaps=!1}else He?(vt&&t.texStorage2D(n.TEXTURE_2D,ae,Pe,ne.width,ne.height),I&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ne.width,ne.height,ge,Ie,ne.data)):t.texImage2D(n.TEXTURE_2D,0,Pe,ne.width,ne.height,0,ge,Ie,ne.data);else if(y.isCompressedTexture)if(y.isCompressedArrayTexture){He&&vt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ae,Pe,nt[0].width,nt[0].height,ne.depth);for(let G=0,Z=nt.length;G<Z;G++)if(ve=nt[G],y.format!==Kn)if(ge!==null)if(He){if(I)if(y.layerUpdates.size>0){let he=Q0(ve.width,ve.height,y.format,y.type);for(let de of y.layerUpdates){let Ue=ve.data.subarray(de*he/ve.data.BYTES_PER_ELEMENT,(de+1)*he/ve.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,G,0,0,de,ve.width,ve.height,1,ge,Ue)}y.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,G,0,0,0,ve.width,ve.height,ne.depth,ge,ve.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,G,Pe,ve.width,ve.height,ne.depth,0,ve.data,0,0);else console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else He?I&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,G,0,0,0,ve.width,ve.height,ne.depth,ge,Ie,ve.data):t.texImage3D(n.TEXTURE_2D_ARRAY,G,Pe,ve.width,ve.height,ne.depth,0,ge,Ie,ve.data)}else{He&&vt&&t.texStorage2D(n.TEXTURE_2D,ae,Pe,nt[0].width,nt[0].height);for(let G=0,Z=nt.length;G<Z;G++)ve=nt[G],y.format!==Kn?ge!==null?He?I&&t.compressedTexSubImage2D(n.TEXTURE_2D,G,0,0,ve.width,ve.height,ge,ve.data):t.compressedTexImage2D(n.TEXTURE_2D,G,Pe,ve.width,ve.height,0,ve.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):He?I&&t.texSubImage2D(n.TEXTURE_2D,G,0,0,ve.width,ve.height,ge,Ie,ve.data):t.texImage2D(n.TEXTURE_2D,G,Pe,ve.width,ve.height,0,ge,Ie,ve.data)}else if(y.isDataArrayTexture)if(He){if(vt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ae,Pe,ne.width,ne.height,ne.depth),I)if(y.layerUpdates.size>0){let G=Q0(ne.width,ne.height,y.format,y.type);for(let Z of y.layerUpdates){let he=ne.data.subarray(Z*G/ne.data.BYTES_PER_ELEMENT,(Z+1)*G/ne.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,Z,ne.width,ne.height,1,ge,Ie,he)}y.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,ne.width,ne.height,ne.depth,ge,Ie,ne.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Pe,ne.width,ne.height,ne.depth,0,ge,Ie,ne.data);else if(y.isData3DTexture)He?(vt&&t.texStorage3D(n.TEXTURE_3D,ae,Pe,ne.width,ne.height,ne.depth),I&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,ne.width,ne.height,ne.depth,ge,Ie,ne.data)):t.texImage3D(n.TEXTURE_3D,0,Pe,ne.width,ne.height,ne.depth,0,ge,Ie,ne.data);else if(y.isFramebufferTexture){if(vt)if(He)t.texStorage2D(n.TEXTURE_2D,ae,Pe,ne.width,ne.height);else{let G=ne.width,Z=ne.height;for(let he=0;he<ae;he++)t.texImage2D(n.TEXTURE_2D,he,Pe,G,Z,0,ge,Ie,null),G>>=1,Z>>=1}}else if(nt.length>0){if(He&&vt){let G=Te(nt[0]);t.texStorage2D(n.TEXTURE_2D,ae,Pe,G.width,G.height)}for(let G=0,Z=nt.length;G<Z;G++)ve=nt[G],He?I&&t.texSubImage2D(n.TEXTURE_2D,G,0,0,ge,Ie,ve):t.texImage2D(n.TEXTURE_2D,G,Pe,ge,Ie,ve);y.generateMipmaps=!1}else if(He){if(vt){let G=Te(ne);t.texStorage2D(n.TEXTURE_2D,ae,Pe,G.width,G.height)}I&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ge,Ie,ne)}else t.texImage2D(n.TEXTURE_2D,0,Pe,ge,Ie,ne);m(y)&&p(Y),Me.__version=q.version,y.onUpdate&&y.onUpdate(y)}w.__version=y.version}function re(w,y,k){if(y.image.length!==6)return;let Y=gt(w,y),ee=y.source;t.bindTexture(n.TEXTURE_CUBE_MAP,w.__webglTexture,n.TEXTURE0+k);let q=i.get(ee);if(ee.version!==q.__version||Y===!0){t.activeTexture(n.TEXTURE0+k);let Me=ot.getPrimaries(ot.workingColorSpace),ue=y.colorSpace===ur?null:ot.getPrimaries(y.colorSpace),me=y.colorSpace===ur||Me===ue?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,y.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,y.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,y.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,me);let st=y.isCompressedTexture||y.image[0].isCompressedTexture,ne=y.image[0]&&y.image[0].isDataTexture,ge=[];for(let Z=0;Z<6;Z++)!st&&!ne?ge[Z]=v(y.image[Z],!0,r.maxCubemapSize):ge[Z]=ne?y.image[Z].image:y.image[Z],ge[Z]=xt(y,ge[Z]);let Ie=ge[0],Pe=s.convert(y.format,y.colorSpace),ve=s.convert(y.type),nt=E(y.internalFormat,Pe,ve,y.colorSpace),He=y.isVideoTexture!==!0,vt=q.__version===void 0||Y===!0,I=ee.dataReady,ae=F(y,Ie);qe(n.TEXTURE_CUBE_MAP,y);let G;if(st){He&&vt&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ae,nt,Ie.width,Ie.height);for(let Z=0;Z<6;Z++){G=ge[Z].mipmaps;for(let he=0;he<G.length;he++){let de=G[he];y.format!==Kn?Pe!==null?He?I&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he,0,0,de.width,de.height,Pe,de.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he,nt,de.width,de.height,0,de.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):He?I&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he,0,0,de.width,de.height,Pe,ve,de.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he,nt,de.width,de.height,0,Pe,ve,de.data)}}}else{if(G=y.mipmaps,He&&vt){G.length>0&&ae++;let Z=Te(ge[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ae,nt,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ne){He?I&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,ge[Z].width,ge[Z].height,Pe,ve,ge[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,nt,ge[Z].width,ge[Z].height,0,Pe,ve,ge[Z].data);for(let he=0;he<G.length;he++){let Ue=G[he].image[Z].image;He?I&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he+1,0,0,Ue.width,Ue.height,Pe,ve,Ue.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he+1,nt,Ue.width,Ue.height,0,Pe,ve,Ue.data)}}else{He?I&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Pe,ve,ge[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,nt,Pe,ve,ge[Z]);for(let he=0;he<G.length;he++){let de=G[he];He?I&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he+1,0,0,Pe,ve,de.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,he+1,nt,Pe,ve,de.image[Z])}}}m(y)&&p(n.TEXTURE_CUBE_MAP),q.__version=ee.version,y.onUpdate&&y.onUpdate(y)}w.__version=y.version}function be(w,y,k,Y,ee,q){let Me=s.convert(k.format,k.colorSpace),ue=s.convert(k.type),me=E(k.internalFormat,Me,ue,k.colorSpace),st=i.get(y),ne=i.get(k);if(ne.__renderTarget=y,!st.__hasExternalTextures){let ge=Math.max(1,y.width>>q),Ie=Math.max(1,y.height>>q);ee===n.TEXTURE_3D||ee===n.TEXTURE_2D_ARRAY?t.texImage3D(ee,q,me,ge,Ie,y.depth,0,Me,ue,null):t.texImage2D(ee,q,me,ge,Ie,0,Me,ue,null)}t.bindFramebuffer(n.FRAMEBUFFER,w),tt(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Y,ee,ne.__webglTexture,0,et(y)):(ee===n.TEXTURE_2D||ee>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&ee<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,Y,ee,ne.__webglTexture,q),t.bindFramebuffer(n.FRAMEBUFFER,null)}function ce(w,y,k){if(n.bindRenderbuffer(n.RENDERBUFFER,w),y.depthBuffer){let Y=y.depthTexture,ee=Y&&Y.isDepthTexture?Y.type:null,q=b(y.stencilBuffer,ee),Me=y.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ue=et(y);tt(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,ue,q,y.width,y.height):k?n.renderbufferStorageMultisample(n.RENDERBUFFER,ue,q,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,q,y.width,y.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,Me,n.RENDERBUFFER,w)}else{let Y=y.textures;for(let ee=0;ee<Y.length;ee++){let q=Y[ee],Me=s.convert(q.format,q.colorSpace),ue=s.convert(q.type),me=E(q.internalFormat,Me,ue,q.colorSpace),st=et(y);k&&tt(y)===!1?n.renderbufferStorageMultisample(n.RENDERBUFFER,st,me,y.width,y.height):tt(y)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,st,me,y.width,y.height):n.renderbufferStorage(n.RENDERBUFFER,me,y.width,y.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Re(w,y){if(y&&y.isWebGLCubeRenderTarget)throw new Error("Depth Texture with cube render targets is not supported");if(t.bindFramebuffer(n.FRAMEBUFFER,w),!(y.depthTexture&&y.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let Y=i.get(y.depthTexture);Y.__renderTarget=y,(!Y.__webglTexture||y.depthTexture.image.width!==y.width||y.depthTexture.image.height!==y.height)&&(y.depthTexture.image.width=y.width,y.depthTexture.image.height=y.height,y.depthTexture.needsUpdate=!0),K(y.depthTexture,0);let ee=Y.__webglTexture,q=et(y);if(y.depthTexture.format===io)tt(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,ee,0,q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,ee,0);else if(y.depthTexture.format===uo)tt(y)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,ee,0,q):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,ee,0);else throw new Error("Unknown depthTexture format")}function ke(w){let y=i.get(w),k=w.isWebGLCubeRenderTarget===!0;if(y.__boundDepthTexture!==w.depthTexture){let Y=w.depthTexture;if(y.__depthDisposeCallback&&y.__depthDisposeCallback(),Y){let ee=()=>{delete y.__boundDepthTexture,delete y.__depthDisposeCallback,Y.removeEventListener("dispose",ee)};Y.addEventListener("dispose",ee),y.__depthDisposeCallback=ee}y.__boundDepthTexture=Y}if(w.depthTexture&&!y.__autoAllocateDepthBuffer){if(k)throw new Error("target.depthTexture not supported in Cube render targets");Re(y.__webglFramebuffer,w)}else if(k){y.__webglDepthbuffer=[];for(let Y=0;Y<6;Y++)if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer[Y]),y.__webglDepthbuffer[Y]===void 0)y.__webglDepthbuffer[Y]=n.createRenderbuffer(),ce(y.__webglDepthbuffer[Y],w,!1);else{let ee=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,q=y.__webglDepthbuffer[Y];n.bindRenderbuffer(n.RENDERBUFFER,q),n.framebufferRenderbuffer(n.FRAMEBUFFER,ee,n.RENDERBUFFER,q)}}else if(t.bindFramebuffer(n.FRAMEBUFFER,y.__webglFramebuffer),y.__webglDepthbuffer===void 0)y.__webglDepthbuffer=n.createRenderbuffer(),ce(y.__webglDepthbuffer,w,!1);else{let Y=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ee=y.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,ee),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,ee)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Xe(w,y,k){let Y=i.get(w);y!==void 0&&be(Y.__webglFramebuffer,w,w.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),k!==void 0&&ke(w)}function wt(w){let y=w.texture,k=i.get(w),Y=i.get(y);w.addEventListener("dispose",C);let ee=w.textures,q=w.isWebGLCubeRenderTarget===!0,Me=ee.length>1;if(Me||(Y.__webglTexture===void 0&&(Y.__webglTexture=n.createTexture()),Y.__version=y.version,o.memory.textures++),q){k.__webglFramebuffer=[];for(let ue=0;ue<6;ue++)if(y.mipmaps&&y.mipmaps.length>0){k.__webglFramebuffer[ue]=[];for(let me=0;me<y.mipmaps.length;me++)k.__webglFramebuffer[ue][me]=n.createFramebuffer()}else k.__webglFramebuffer[ue]=n.createFramebuffer()}else{if(y.mipmaps&&y.mipmaps.length>0){k.__webglFramebuffer=[];for(let ue=0;ue<y.mipmaps.length;ue++)k.__webglFramebuffer[ue]=n.createFramebuffer()}else k.__webglFramebuffer=n.createFramebuffer();if(Me)for(let ue=0,me=ee.length;ue<me;ue++){let st=i.get(ee[ue]);st.__webglTexture===void 0&&(st.__webglTexture=n.createTexture(),o.memory.textures++)}if(w.samples>0&&tt(w)===!1){k.__webglMultisampledFramebuffer=n.createFramebuffer(),k.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,k.__webglMultisampledFramebuffer);for(let ue=0;ue<ee.length;ue++){let me=ee[ue];k.__webglColorRenderbuffer[ue]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,k.__webglColorRenderbuffer[ue]);let st=s.convert(me.format,me.colorSpace),ne=s.convert(me.type),ge=E(me.internalFormat,st,ne,me.colorSpace,w.isXRRenderTarget===!0),Ie=et(w);n.renderbufferStorageMultisample(n.RENDERBUFFER,Ie,ge,w.width,w.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ue,n.RENDERBUFFER,k.__webglColorRenderbuffer[ue])}n.bindRenderbuffer(n.RENDERBUFFER,null),w.depthBuffer&&(k.__webglDepthRenderbuffer=n.createRenderbuffer(),ce(k.__webglDepthRenderbuffer,w,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(q){t.bindTexture(n.TEXTURE_CUBE_MAP,Y.__webglTexture),qe(n.TEXTURE_CUBE_MAP,y);for(let ue=0;ue<6;ue++)if(y.mipmaps&&y.mipmaps.length>0)for(let me=0;me<y.mipmaps.length;me++)be(k.__webglFramebuffer[ue][me],w,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+ue,me);else be(k.__webglFramebuffer[ue],w,y,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+ue,0);m(y)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(Me){for(let ue=0,me=ee.length;ue<me;ue++){let st=ee[ue],ne=i.get(st);t.bindTexture(n.TEXTURE_2D,ne.__webglTexture),qe(n.TEXTURE_2D,st),be(k.__webglFramebuffer,w,st,n.COLOR_ATTACHMENT0+ue,n.TEXTURE_2D,0),m(st)&&p(n.TEXTURE_2D)}t.unbindTexture()}else{let ue=n.TEXTURE_2D;if((w.isWebGL3DRenderTarget||w.isWebGLArrayRenderTarget)&&(ue=w.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ue,Y.__webglTexture),qe(ue,y),y.mipmaps&&y.mipmaps.length>0)for(let me=0;me<y.mipmaps.length;me++)be(k.__webglFramebuffer[me],w,y,n.COLOR_ATTACHMENT0,ue,me);else be(k.__webglFramebuffer,w,y,n.COLOR_ATTACHMENT0,ue,0);m(y)&&p(ue),t.unbindTexture()}w.depthBuffer&&ke(w)}function rt(w){let y=w.textures;for(let k=0,Y=y.length;k<Y;k++){let ee=y[k];if(m(ee)){let q=S(w),Me=i.get(ee).__webglTexture;t.bindTexture(q,Me),p(q),t.unbindTexture()}}}let Ft=[],P=[];function Tn(w){if(w.samples>0){if(tt(w)===!1){let y=w.textures,k=w.width,Y=w.height,ee=n.COLOR_BUFFER_BIT,q=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Me=i.get(w),ue=y.length>1;if(ue)for(let me=0;me<y.length;me++)t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,Me.__webglMultisampledFramebuffer),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglFramebuffer);for(let me=0;me<y.length;me++){if(w.resolveDepthBuffer&&(w.depthBuffer&&(ee|=n.DEPTH_BUFFER_BIT),w.stencilBuffer&&w.resolveStencilBuffer&&(ee|=n.STENCIL_BUFFER_BIT)),ue){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,Me.__webglColorRenderbuffer[me]);let st=i.get(y[me]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,st,0)}n.blitFramebuffer(0,0,k,Y,0,0,k,Y,ee,n.NEAREST),l===!0&&(Ft.length=0,P.length=0,Ft.push(n.COLOR_ATTACHMENT0+me),w.depthBuffer&&w.resolveDepthBuffer===!1&&(Ft.push(q),P.push(q),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,P)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Ft))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),ue)for(let me=0;me<y.length;me++){t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.RENDERBUFFER,Me.__webglColorRenderbuffer[me]);let st=i.get(y[me]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.TEXTURE_2D,st,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglMultisampledFramebuffer)}else if(w.depthBuffer&&w.resolveDepthBuffer===!1&&l){let y=w.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[y])}}}function et(w){return Math.min(r.maxSamples,w.samples)}function tt(w){let y=i.get(w);return w.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&y.__useRenderToTexture!==!1}function Ae(w){let y=o.render.frame;u.get(w)!==y&&(u.set(w,y),w.update())}function xt(w,y){let k=w.colorSpace,Y=w.format,ee=w.type;return w.isCompressedTexture===!0||w.isVideoTexture===!0||k!==go&&k!==ur&&(ot.getTransfer(k)===pt?(Y!==Kn||ee!==Gi)&&console.warn("THREE.WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):console.error("THREE.WebGLTextures: Unsupported texture color space:",k)),y}function Te(w){return typeof HTMLImageElement<"u"&&w instanceof HTMLImageElement?(c.width=w.naturalWidth||w.width,c.height=w.naturalHeight||w.height):typeof VideoFrame<"u"&&w instanceof VideoFrame?(c.width=w.displayWidth,c.height=w.displayHeight):(c.width=w.width,c.height=w.height),c}this.allocateTextureUnit=H,this.resetTextureUnits=z,this.setTexture2D=K,this.setTexture2DArray=j,this.setTexture3D=te,this.setTextureCube=W,this.rebindTextures=Xe,this.setupRenderTarget=wt,this.updateRenderTargetMipmap=rt,this.updateMultisampleRenderTarget=Tn,this.setupDepthRenderbuffer=ke,this.setupFrameBufferTexture=be,this.useMultisampledRTT=tt}function SN(n,e){function t(i,r=ur){let s,o=ot.getTransfer(r);if(i===Gi)return n.UNSIGNED_BYTE;if(i===vm)return n.UNSIGNED_SHORT_4_4_4_4;if(i===ym)return n.UNSIGNED_SHORT_5_5_5_1;if(i===ux)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===lx)return n.BYTE;if(i===cx)return n.SHORT;if(i===_a)return n.UNSIGNED_SHORT;if(i===gm)return n.INT;if(i===as)return n.UNSIGNED_INT;if(i===Vi)return n.FLOAT;if(i===Sa)return n.HALF_FLOAT;if(i===dx)return n.ALPHA;if(i===fx)return n.RGB;if(i===Kn)return n.RGBA;if(i===hx)return n.LUMINANCE;if(i===px)return n.LUMINANCE_ALPHA;if(i===io)return n.DEPTH_COMPONENT;if(i===uo)return n.DEPTH_STENCIL;if(i===mx)return n.RED;if(i===_m)return n.RED_INTEGER;if(i===gx)return n.RG;if(i===xm)return n.RG_INTEGER;if(i===bm)return n.RGBA_INTEGER;if(i===kc||i===Uc||i===Vc||i===Bc)if(o===pt)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===kc)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Uc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Vc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Bc)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===kc)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Uc)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Vc)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Bc)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===hp||i===pp||i===mp||i===gp)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===hp)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===pp)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===mp)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===gp)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===vp||i===yp||i===_p)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===vp||i===yp)return o===pt?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===_p)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC}else return null;if(i===xp||i===bp||i===Mp||i===Ep||i===Sp||i===wp||i===Cp||i===Tp||i===Dp||i===Ap||i===Ip||i===Rp||i===Np||i===Pp)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===xp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===bp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===Mp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Ep)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Sp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===wp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Cp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Tp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Dp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Ap)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Ip)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Rp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Np)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Pp)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Hc||i===Op||i===Fp)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Hc)return o===pt?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Op)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Fp)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===vx||i===Lp||i===kp||i===Up)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Hc)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Lp)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===kp)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Up)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===co?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var tm=class extends nn{constructor(e=[]){super(),this.isArrayCamera=!0,this.cameras=e}},Hi=class extends vr{constructor(){super(),this.isGroup=!0,this.type="Group"}},wN={type:"move"},ya=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Hi,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Hi,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new O,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new O),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Hi,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new O,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new O),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,l=this._grip,c=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(c&&e.hand){o=!0;for(let v of e.hand.values()){let m=t.getJointPose(v,i),p=this._getHandJoint(c,v);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=c.joints["index-finger-tip"],d=c.joints["thumb-tip"],h=u.position.distanceTo(d.position),f=.02,g=.005;c.inputState.pinching&&h>f+g?(c.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!c.inputState.pinching&&h<=f-g&&(c.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else l!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(l.matrix.fromArray(s.transform.matrix),l.matrix.decompose(l.position,l.rotation,l.scale),l.matrixWorldNeedsUpdate=!0,s.linearVelocity?(l.hasLinearVelocity=!0,l.linearVelocity.copy(s.linearVelocity)):l.hasLinearVelocity=!1,s.angularVelocity?(l.hasAngularVelocity=!0,l.angularVelocity.copy(s.angularVelocity)):l.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(wN)))}return a!==null&&(a.visible=r!==null),l!==null&&(l.visible=s!==null),c!==null&&(c.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Hi;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},CN=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,TN=`
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

}`,nm=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t,i){if(this.texture===null){let r=new ps,s=e.properties.get(r);s.__webglTexture=t.texture,(t.depthNear!=i.depthNear||t.depthFar!=i.depthFar)&&(this.depthNear=t.depthNear,this.depthFar=t.depthFar),this.texture=r}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new di({vertexShader:CN,fragmentShader:TN,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Xt(new Qc(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},im=class extends pr{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",l=1,c=null,u=null,d=null,h=null,f=null,g=null,v=new nm,m=t.getContextAttributes(),p=null,S=null,E=[],b=[],F=new Qe,T=null,C=new nn;C.viewport=new mt;let A=new nn;A.viewport=new mt;let M=[C,A],_=new tm,D=null,z=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(X){let re=E[X];return re===void 0&&(re=new ya,E[X]=re),re.getTargetRaySpace()},this.getControllerGrip=function(X){let re=E[X];return re===void 0&&(re=new ya,E[X]=re),re.getGripSpace()},this.getHand=function(X){let re=E[X];return re===void 0&&(re=new ya,E[X]=re),re.getHandSpace()};function H(X){let re=b.indexOf(X.inputSource);if(re===-1)return;let be=E[re];be!==void 0&&(be.update(X.inputSource,X.frame,c||o),be.dispatchEvent({type:X.type,data:X.inputSource}))}function $(){r.removeEventListener("select",H),r.removeEventListener("selectstart",H),r.removeEventListener("selectend",H),r.removeEventListener("squeeze",H),r.removeEventListener("squeezestart",H),r.removeEventListener("squeezeend",H),r.removeEventListener("end",$),r.removeEventListener("inputsourceschange",K);for(let X=0;X<E.length;X++){let re=b[X];re!==null&&(b[X]=null,E[X].disconnect(re))}D=null,z=null,v.reset(),e.setRenderTarget(p),f=null,h=null,d=null,r=null,S=null,gt.stop(),i.isPresenting=!1,e.setPixelRatio(T),e.setSize(F.width,F.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(X){s=X,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(X){a=X,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return c||o},this.setReferenceSpace=function(X){c=X},this.getBaseLayer=function(){return h!==null?h:f},this.getBinding=function(){return d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function(X){return vs(this,null,function*(){if(r=X,r!==null){if(p=e.getRenderTarget(),r.addEventListener("select",H),r.addEventListener("selectstart",H),r.addEventListener("selectend",H),r.addEventListener("squeeze",H),r.addEventListener("squeezestart",H),r.addEventListener("squeezeend",H),r.addEventListener("end",$),r.addEventListener("inputsourceschange",K),m.xrCompatible!==!0&&(yield t.makeXRCompatible()),T=e.getPixelRatio(),e.getSize(F),r.renderState.layers===void 0){let re={antialias:m.antialias,alpha:!0,depth:m.depth,stencil:m.stencil,framebufferScaleFactor:s};f=new XRWebGLLayer(r,t,re),r.updateRenderState({baseLayer:f}),e.setPixelRatio(1),e.setSize(f.framebufferWidth,f.framebufferHeight,!1),S=new Wi(f.framebufferWidth,f.framebufferHeight,{format:Kn,type:Gi,colorSpace:e.outputColorSpace,stencilBuffer:m.stencil})}else{let re=null,be=null,ce=null;m.depth&&(ce=m.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,re=m.stencil?uo:io,be=m.stencil?co:as);let Re={colorFormat:t.RGBA8,depthFormat:ce,scaleFactor:s};d=new XRWebGLBinding(r,t),h=d.createProjectionLayer(Re),r.updateRenderState({layers:[h]}),e.setPixelRatio(1),e.setSize(h.textureWidth,h.textureHeight,!1),S=new Wi(h.textureWidth,h.textureHeight,{format:Kn,type:Gi,depthTexture:new tu(h.textureWidth,h.textureHeight,be,void 0,void 0,void 0,void 0,void 0,void 0,re),stencilBuffer:m.stencil,colorSpace:e.outputColorSpace,samples:m.antialias?4:0,resolveDepthBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(l),c=null,o=yield r.requestReferenceSpace(a),gt.setContext(r),gt.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return v.getDepthTexture()};function K(X){for(let re=0;re<X.removed.length;re++){let be=X.removed[re],ce=b.indexOf(be);ce>=0&&(b[ce]=null,E[ce].disconnect(be))}for(let re=0;re<X.added.length;re++){let be=X.added[re],ce=b.indexOf(be);if(ce===-1){for(let ke=0;ke<E.length;ke++)if(ke>=b.length){b.push(be),ce=ke;break}else if(b[ke]===null){b[ke]=be,ce=ke;break}if(ce===-1)break}let Re=E[ce];Re&&Re.connect(be)}}let j=new O,te=new O;function W(X,re,be){j.setFromMatrixPosition(re.matrixWorld),te.setFromMatrixPosition(be.matrixWorld);let ce=j.distanceTo(te),Re=re.projectionMatrix.elements,ke=be.projectionMatrix.elements,Xe=Re[14]/(Re[10]-1),wt=Re[14]/(Re[10]+1),rt=(Re[9]+1)/Re[5],Ft=(Re[9]-1)/Re[5],P=(Re[8]-1)/Re[0],Tn=(ke[8]+1)/ke[0],et=Xe*P,tt=Xe*Tn,Ae=ce/(-P+Tn),xt=Ae*-P;if(re.matrixWorld.decompose(X.position,X.quaternion,X.scale),X.translateX(xt),X.translateZ(Ae),X.matrixWorld.compose(X.position,X.quaternion,X.scale),X.matrixWorldInverse.copy(X.matrixWorld).invert(),Re[10]===-1)X.projectionMatrix.copy(re.projectionMatrix),X.projectionMatrixInverse.copy(re.projectionMatrixInverse);else{let Te=Xe+Ae,w=wt+Ae,y=et-xt,k=tt+(ce-xt),Y=rt*wt/w*Te,ee=Ft*wt/w*Te;X.projectionMatrix.makePerspective(y,k,Y,ee,Te,w),X.projectionMatrixInverse.copy(X.projectionMatrix).invert()}}function le(X,re){re===null?X.matrixWorld.copy(X.matrix):X.matrixWorld.multiplyMatrices(re.matrixWorld,X.matrix),X.matrixWorldInverse.copy(X.matrixWorld).invert()}this.updateCamera=function(X){if(r===null)return;let re=X.near,be=X.far;v.texture!==null&&(v.depthNear>0&&(re=v.depthNear),v.depthFar>0&&(be=v.depthFar)),_.near=A.near=C.near=re,_.far=A.far=C.far=be,(D!==_.near||z!==_.far)&&(r.updateRenderState({depthNear:_.near,depthFar:_.far}),D=_.near,z=_.far),C.layers.mask=X.layers.mask|2,A.layers.mask=X.layers.mask|4,_.layers.mask=C.layers.mask|A.layers.mask;let ce=X.parent,Re=_.cameras;le(_,ce);for(let ke=0;ke<Re.length;ke++)le(Re[ke],ce);Re.length===2?W(_,C,A):_.projectionMatrix.copy(C.projectionMatrix),pe(X,_,ce)};function pe(X,re,be){be===null?X.matrix.copy(re.matrixWorld):(X.matrix.copy(be.matrixWorld),X.matrix.invert(),X.matrix.multiply(re.matrixWorld)),X.matrix.decompose(X.position,X.quaternion,X.scale),X.updateMatrixWorld(!0),X.projectionMatrix.copy(re.projectionMatrix),X.projectionMatrixInverse.copy(re.projectionMatrixInverse),X.isPerspectiveCamera&&(X.fov=Bp*2*Math.atan(1/X.projectionMatrix.elements[5]),X.zoom=1)}this.getCamera=function(){return _},this.getFoveation=function(){if(!(h===null&&f===null))return l},this.setFoveation=function(X){l=X,h!==null&&(h.fixedFoveation=X),f!==null&&f.fixedFoveation!==void 0&&(f.fixedFoveation=X)},this.hasDepthSensing=function(){return v.texture!==null},this.getDepthSensingMesh=function(){return v.getMesh(_)};let Ce=null;function qe(X,re){if(u=re.getViewerPose(c||o),g=re,u!==null){let be=u.views;f!==null&&(e.setRenderTargetFramebuffer(S,f.framebuffer),e.setRenderTarget(S));let ce=!1;be.length!==_.cameras.length&&(_.cameras.length=0,ce=!0);for(let ke=0;ke<be.length;ke++){let Xe=be[ke],wt=null;if(f!==null)wt=f.getViewport(Xe);else{let Ft=d.getViewSubImage(h,Xe);wt=Ft.viewport,ke===0&&(e.setRenderTargetTextures(S,Ft.colorTexture,h.ignoreDepthValues?void 0:Ft.depthStencilTexture),e.setRenderTarget(S))}let rt=M[ke];rt===void 0&&(rt=new nn,rt.layers.enable(ke),rt.viewport=new mt,M[ke]=rt),rt.matrix.fromArray(Xe.transform.matrix),rt.matrix.decompose(rt.position,rt.quaternion,rt.scale),rt.projectionMatrix.fromArray(Xe.projectionMatrix),rt.projectionMatrixInverse.copy(rt.projectionMatrix).invert(),rt.viewport.set(wt.x,wt.y,wt.width,wt.height),ke===0&&(_.matrix.copy(rt.matrix),_.matrix.decompose(_.position,_.quaternion,_.scale)),ce===!0&&_.cameras.push(rt)}let Re=r.enabledFeatures;if(Re&&Re.includes("depth-sensing")){let ke=d.getDepthInformation(be[0]);ke&&ke.isValid&&ke.texture&&v.init(e,ke,r.renderState)}}for(let be=0;be<E.length;be++){let ce=b[be],Re=E[be];ce!==null&&Re!==void 0&&Re.update(ce,re,c||o)}Ce&&Ce(X,re),re.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:re}),g=null}let gt=new Ex;gt.setAnimationLoop(qe),this.setAnimationLoop=function(X){Ce=X},this.dispose=function(){}}},es=new cs,DN=new At;function AN(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,Mx(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,S,E,b){p.isMeshBasicMaterial||p.isMeshLambertMaterial?s(m,p):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p)):p.isMeshStandardMaterial?(s(m,p),h(m,p),p.isMeshPhysicalMaterial&&f(m,p,b)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),v(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?l(m,p,S,E):p.isSpriteMaterial?c(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===bn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===bn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let S=e.get(p),E=S.envMap,b=S.envMapRotation;E&&(m.envMap.value=E,es.copy(b),es.x*=-1,es.y*=-1,es.z*=-1,E.isCubeTexture&&E.isRenderTargetTexture===!1&&(es.y*=-1,es.z*=-1),m.envMapRotation.value.setFromMatrix4(DN.makeRotationFromEuler(es)),m.flipEnvMap.value=E.isCubeTexture&&E.isRenderTargetTexture===!1?-1:1,m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function l(m,p,S,E){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*S,m.scale.value=E*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function c(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function h(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function f(m,p,S){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===bn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=S.texture,m.transmissionSamplerSize.value.set(S.width,S.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function v(m,p){let S=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(S.matrixWorld),m.nearDistance.value=S.shadow.camera.near,m.farDistance.value=S.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function IN(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function l(S,E){let b=E.program;i.uniformBlockBinding(S,b)}function c(S,E){let b=r[S.id];b===void 0&&(g(S),b=u(S),r[S.id]=b,S.addEventListener("dispose",m));let F=E.program;i.updateUBOMapping(S,F);let T=e.render.frame;s[S.id]!==T&&(h(S),s[S.id]=T)}function u(S){let E=d();S.__bindingPointIndex=E;let b=n.createBuffer(),F=S.__size,T=S.usage;return n.bindBuffer(n.UNIFORM_BUFFER,b),n.bufferData(n.UNIFORM_BUFFER,F,T),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,E,b),b}function d(){for(let S=0;S<a;S++)if(o.indexOf(S)===-1)return o.push(S),S;return console.error("THREE.WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function h(S){let E=r[S.id],b=S.uniforms,F=S.__cache;n.bindBuffer(n.UNIFORM_BUFFER,E);for(let T=0,C=b.length;T<C;T++){let A=Array.isArray(b[T])?b[T]:[b[T]];for(let M=0,_=A.length;M<_;M++){let D=A[M];if(f(D,T,M,F)===!0){let z=D.__offset,H=Array.isArray(D.value)?D.value:[D.value],$=0;for(let K=0;K<H.length;K++){let j=H[K],te=v(j);typeof j=="number"||typeof j=="boolean"?(D.__data[0]=j,n.bufferSubData(n.UNIFORM_BUFFER,z+$,D.__data)):j.isMatrix3?(D.__data[0]=j.elements[0],D.__data[1]=j.elements[1],D.__data[2]=j.elements[2],D.__data[3]=0,D.__data[4]=j.elements[3],D.__data[5]=j.elements[4],D.__data[6]=j.elements[5],D.__data[7]=0,D.__data[8]=j.elements[6],D.__data[9]=j.elements[7],D.__data[10]=j.elements[8],D.__data[11]=0):(j.toArray(D.__data,$),$+=te.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,z,D.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function f(S,E,b,F){let T=S.value,C=E+"_"+b;if(F[C]===void 0)return typeof T=="number"||typeof T=="boolean"?F[C]=T:F[C]=T.clone(),!0;{let A=F[C];if(typeof T=="number"||typeof T=="boolean"){if(A!==T)return F[C]=T,!0}else if(A.equals(T)===!1)return A.copy(T),!0}return!1}function g(S){let E=S.uniforms,b=0,F=16;for(let C=0,A=E.length;C<A;C++){let M=Array.isArray(E[C])?E[C]:[E[C]];for(let _=0,D=M.length;_<D;_++){let z=M[_],H=Array.isArray(z.value)?z.value:[z.value];for(let $=0,K=H.length;$<K;$++){let j=H[$],te=v(j),W=b%F,le=W%te.boundary,pe=W+le;b+=le,pe!==0&&F-pe<te.storage&&(b+=F-pe),z.__data=new Float32Array(te.storage/Float32Array.BYTES_PER_ELEMENT),z.__offset=b,b+=te.storage}}}let T=b%F;return T>0&&(b+=F-T),S.__size=b,S.__cache={},this}function v(S){let E={boundary:0,storage:0};return typeof S=="number"||typeof S=="boolean"?(E.boundary=4,E.storage=4):S.isVector2?(E.boundary=8,E.storage=8):S.isVector3||S.isColor?(E.boundary=16,E.storage=12):S.isVector4?(E.boundary=16,E.storage=16):S.isMatrix3?(E.boundary=48,E.storage=48):S.isMatrix4?(E.boundary=64,E.storage=64):S.isTexture?console.warn("THREE.WebGLRenderer: Texture samplers can not be part of an uniforms group."):console.warn("THREE.WebGLRenderer: Unsupported uniform value type.",S),E}function m(S){let E=S.target;E.removeEventListener("dispose",m);let b=o.indexOf(E.__bindingPointIndex);o.splice(b,1),n.deleteBuffer(r[E.id]),delete r[E.id],delete s[E.id]}function p(){for(let S in r)n.deleteBuffer(r[S]);o=[],r={},s={}}return{bind:l,update:c,dispose:p}}var nu=class{constructor(e={}){let{canvas:t=OD(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:l=!0,preserveDrawingBuffer:c=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reverseDepthBuffer:h=!1}=e;this.isWebGLRenderer=!0;let f;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");f=i.getContextAttributes().alpha}else f=o;let g=new Uint32Array(4),v=new Int32Array(4),m=null,p=null,S=[],E=[];this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this._outputColorSpace=Vn,this.toneMapping=fr,this.toneMappingExposure=1;let b=this,F=!1,T=0,C=0,A=null,M=-1,_=null,D=new mt,z=new mt,H=null,$=new $e(0),K=0,j=t.width,te=t.height,W=1,le=null,pe=null,Ce=new mt(0,0,j,te),qe=new mt(0,0,j,te),gt=!1,X=new ba,re=!1,be=!1,ce=new At,Re=new At,ke=new O,Xe=new mt,wt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},rt=!1;function Ft(){return A===null?W:1}let P=i;function Tn(x,R){return t.getContext(x,R)}try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:l,preserveDrawingBuffer:c,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine","three.js r170"),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",he,!1),t.addEventListener("webglcontextcreationerror",de,!1),P===null){let R="webgl2";if(P=Tn(R,x),P===null)throw Tn(R)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw console.error("THREE.WebGLRenderer: "+x.message),x}let et,tt,Ae,xt,Te,w,y,k,Y,ee,q,Me,ue,me,st,ne,ge,Ie,Pe,ve,nt,He,vt,I;function ae(){et=new $1(P),et.init(),He=new SN(P,et),tt=new B1(P,et,e,He),Ae=new bN(P,et),tt.reverseDepthBuffer&&h&&Ae.buffers.depth.setReversed(!0),xt=new Y1(P),Te=new lN,w=new EN(P,et,Ae,Te,tt,He,xt),y=new z1(b),k=new j1(b),Y=new nA(P),vt=new U1(P,Y),ee=new q1(P,Y,xt,vt),q=new J1(P,ee,Y,xt),Pe=new Z1(P,tt,w),ne=new H1(Te),Me=new aN(b,y,k,et,tt,vt,ne),ue=new AN(b,Te),me=new uN,st=new gN(et),Ie=new k1(b,y,k,Ae,q,f,l),ge=new _N(b,q,tt),I=new IN(P,xt,tt,Ae),ve=new V1(P,et,xt),nt=new X1(P,et,xt),xt.programs=Me.programs,b.capabilities=tt,b.extensions=et,b.properties=Te,b.renderLists=me,b.shadowMap=ge,b.state=Ae,b.info=xt}ae();let G=new im(b,P);this.xr=G,this.getContext=function(){return P},this.getContextAttributes=function(){return P.getContextAttributes()},this.forceContextLoss=function(){let x=et.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=et.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return W},this.setPixelRatio=function(x){x!==void 0&&(W=x,this.setSize(j,te,!1))},this.getSize=function(x){return x.set(j,te)},this.setSize=function(x,R,V=!0){if(G.isPresenting){console.warn("THREE.WebGLRenderer: Can't change size while VR device is presenting.");return}j=x,te=R,t.width=Math.floor(x*W),t.height=Math.floor(R*W),V===!0&&(t.style.width=x+"px",t.style.height=R+"px"),this.setViewport(0,0,x,R)},this.getDrawingBufferSize=function(x){return x.set(j*W,te*W).floor()},this.setDrawingBufferSize=function(x,R,V){j=x,te=R,W=V,t.width=Math.floor(x*V),t.height=Math.floor(R*V),this.setViewport(0,0,x,R)},this.getCurrentViewport=function(x){return x.copy(D)},this.getViewport=function(x){return x.copy(Ce)},this.setViewport=function(x,R,V,B){x.isVector4?Ce.set(x.x,x.y,x.z,x.w):Ce.set(x,R,V,B),Ae.viewport(D.copy(Ce).multiplyScalar(W).round())},this.getScissor=function(x){return x.copy(qe)},this.setScissor=function(x,R,V,B){x.isVector4?qe.set(x.x,x.y,x.z,x.w):qe.set(x,R,V,B),Ae.scissor(z.copy(qe).multiplyScalar(W).round())},this.getScissorTest=function(){return gt},this.setScissorTest=function(x){Ae.setScissorTest(gt=x)},this.setOpaqueSort=function(x){le=x},this.setTransparentSort=function(x){pe=x},this.getClearColor=function(x){return x.copy(Ie.getClearColor())},this.setClearColor=function(){Ie.setClearColor.apply(Ie,arguments)},this.getClearAlpha=function(){return Ie.getClearAlpha()},this.setClearAlpha=function(){Ie.setClearAlpha.apply(Ie,arguments)},this.clear=function(x=!0,R=!0,V=!0){let B=0;if(x){let N=!1;if(A!==null){let ie=A.texture.format;N=ie===bm||ie===xm||ie===_m}if(N){let ie=A.texture.type,fe=ie===Gi||ie===as||ie===_a||ie===co||ie===vm||ie===ym,ye=Ie.getClearColor(),_e=Ie.getClearAlpha(),Fe=ye.r,Ve=ye.g,xe=ye.b;fe?(g[0]=Fe,g[1]=Ve,g[2]=xe,g[3]=_e,P.clearBufferuiv(P.COLOR,0,g)):(v[0]=Fe,v[1]=Ve,v[2]=xe,v[3]=_e,P.clearBufferiv(P.COLOR,0,v))}else B|=P.COLOR_BUFFER_BIT}R&&(B|=P.DEPTH_BUFFER_BIT),V&&(B|=P.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),P.clear(B)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",he,!1),t.removeEventListener("webglcontextcreationerror",de,!1),me.dispose(),st.dispose(),Te.dispose(),y.dispose(),k.dispose(),q.dispose(),vt.dispose(),I.dispose(),Me.dispose(),G.dispose(),G.removeEventListener("sessionstart",Om),G.removeEventListener("sessionend",Fm),br.stop()};function Z(x){x.preventDefault(),console.log("THREE.WebGLRenderer: Context Lost."),F=!0}function he(){console.log("THREE.WebGLRenderer: Context Restored."),F=!1;let x=xt.autoReset,R=ge.enabled,V=ge.autoUpdate,B=ge.needsUpdate,N=ge.type;ae(),xt.autoReset=x,ge.enabled=R,ge.autoUpdate=V,ge.needsUpdate=B,ge.type=N}function de(x){console.error("THREE.WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function Ue(x){let R=x.target;R.removeEventListener("dispose",Ue),It(R)}function It(x){Zt(x),Te.remove(x)}function Zt(x){let R=Te.get(x).programs;R!==void 0&&(R.forEach(function(V){Me.releaseProgram(V)}),x.isShaderMaterial&&Me.releaseShaderCache(x))}this.renderBufferDirect=function(x,R,V,B,N,ie){R===null&&(R=wt);let fe=N.isMesh&&N.matrixWorld.determinant()<0,ye=Yx(x,R,V,B,N);Ae.setMaterial(B,fe);let _e=V.index,Fe=1;if(B.wireframe===!0){if(_e=ee.getWireframeAttribute(V),_e===void 0)return;Fe=2}let Ve=V.drawRange,xe=V.attributes.position,at=Ve.start*Fe,yt=(Ve.start+Ve.count)*Fe;ie!==null&&(at=Math.max(at,ie.start*Fe),yt=Math.min(yt,(ie.start+ie.count)*Fe)),_e!==null?(at=Math.max(at,0),yt=Math.min(yt,_e.count)):xe!=null&&(at=Math.max(at,0),yt=Math.min(yt,xe.count));let bt=yt-at;if(bt<0||bt===1/0)return;vt.setup(N,B,ye,V,_e);let cn,lt=ve;if(_e!==null&&(cn=Y.get(_e),lt=nt,lt.setIndex(cn)),N.isMesh)B.wireframe===!0?(Ae.setLineWidth(B.wireframeLinewidth*Ft()),lt.setMode(P.LINES)):lt.setMode(P.TRIANGLES);else if(N.isLine){let Ee=B.linewidth;Ee===void 0&&(Ee=1),Ae.setLineWidth(Ee*Ft()),N.isLineSegments?lt.setMode(P.LINES):N.isLineLoop?lt.setMode(P.LINE_LOOP):lt.setMode(P.LINE_STRIP)}else N.isPoints?lt.setMode(P.POINTS):N.isSprite&&lt.setMode(P.TRIANGLES);if(N.isBatchedMesh)if(N._multiDrawInstances!==null)lt.renderMultiDrawInstances(N._multiDrawStarts,N._multiDrawCounts,N._multiDrawCount,N._multiDrawInstances);else if(et.get("WEBGL_multi_draw"))lt.renderMultiDraw(N._multiDrawStarts,N._multiDrawCounts,N._multiDrawCount);else{let Ee=N._multiDrawStarts,pi=N._multiDrawCounts,ct=N._multiDrawCount,Hn=_e?Y.get(_e).bytesPerElement:1,gs=Te.get(B).currentProgram.getUniforms();for(let Mn=0;Mn<ct;Mn++)gs.setValue(P,"_gl_DrawID",Mn),lt.render(Ee[Mn]/Hn,pi[Mn])}else if(N.isInstancedMesh)lt.renderInstances(at,bt,N.count);else if(V.isInstancedBufferGeometry){let Ee=V._maxInstanceCount!==void 0?V._maxInstanceCount:1/0,pi=Math.min(V.instanceCount,Ee);lt.renderInstances(at,bt,pi)}else lt.render(at,bt)};function ut(x,R,V){x.transparent===!0&&x.side===Ui&&x.forceSinglePass===!1?(x.side=bn,x.needsUpdate=!0,Oa(x,R,V),x.side=hr,x.needsUpdate=!0,Oa(x,R,V),x.side=Ui):Oa(x,R,V)}this.compile=function(x,R,V=null){V===null&&(V=x),p=st.get(V),p.init(R),E.push(p),V.traverseVisible(function(N){N.isLight&&N.layers.test(R.layers)&&(p.pushLight(N),N.castShadow&&p.pushShadow(N))}),x!==V&&x.traverseVisible(function(N){N.isLight&&N.layers.test(R.layers)&&(p.pushLight(N),N.castShadow&&p.pushShadow(N))}),p.setupLights();let B=new Set;return x.traverse(function(N){if(!(N.isMesh||N.isPoints||N.isLine||N.isSprite))return;let ie=N.material;if(ie)if(Array.isArray(ie))for(let fe=0;fe<ie.length;fe++){let ye=ie[fe];ut(ye,V,N),B.add(ye)}else ut(ie,V,N),B.add(ie)}),E.pop(),p=null,B},this.compileAsync=function(x,R,V=null){let B=this.compile(x,R,V);return new Promise(N=>{function ie(){if(B.forEach(function(fe){Te.get(fe).currentProgram.isReady()&&B.delete(fe)}),B.size===0){N(x);return}setTimeout(ie,10)}et.get("KHR_parallel_shader_compile")!==null?ie():setTimeout(ie,10)})};let Bn=null;function hi(x){Bn&&Bn(x)}function Om(){br.stop()}function Fm(){br.start()}let br=new Ex;br.setAnimationLoop(hi),typeof self<"u"&&br.setContext(self),this.setAnimationLoop=function(x){Bn=x,G.setAnimationLoop(x),x===null?br.stop():br.start()},G.addEventListener("sessionstart",Om),G.addEventListener("sessionend",Fm),this.render=function(x,R){if(R!==void 0&&R.isCamera!==!0){console.error("THREE.WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(F===!0)return;if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),R.parent===null&&R.matrixWorldAutoUpdate===!0&&R.updateMatrixWorld(),G.enabled===!0&&G.isPresenting===!0&&(G.cameraAutoUpdate===!0&&G.updateCamera(R),R=G.getCamera()),x.isScene===!0&&x.onBeforeRender(b,x,R,A),p=st.get(x,E.length),p.init(R),E.push(p),Re.multiplyMatrices(R.projectionMatrix,R.matrixWorldInverse),X.setFromProjectionMatrix(Re),be=this.localClippingEnabled,re=ne.init(this.clippingPlanes,be),m=me.get(x,S.length),m.init(),S.push(m),G.enabled===!0&&G.isPresenting===!0){let ie=b.xr.getDepthSensingMesh();ie!==null&&Ou(ie,R,-1/0,b.sortObjects)}Ou(x,R,0,b.sortObjects),m.finish(),b.sortObjects===!0&&m.sort(le,pe),rt=G.enabled===!1||G.isPresenting===!1||G.hasDepthSensing()===!1,rt&&Ie.addToRenderList(m,x),this.info.render.frame++,re===!0&&ne.beginShadows();let V=p.state.shadowsArray;ge.render(V,x,R),re===!0&&ne.endShadows(),this.info.autoReset===!0&&this.info.reset();let B=m.opaque,N=m.transmissive;if(p.setupLights(),R.isArrayCamera){let ie=R.cameras;if(N.length>0)for(let fe=0,ye=ie.length;fe<ye;fe++){let _e=ie[fe];km(B,N,x,_e)}rt&&Ie.render(x);for(let fe=0,ye=ie.length;fe<ye;fe++){let _e=ie[fe];Lm(m,x,_e,_e.viewport)}}else N.length>0&&km(B,N,x,R),rt&&Ie.render(x),Lm(m,x,R);A!==null&&(w.updateMultisampleRenderTarget(A),w.updateRenderTargetMipmap(A)),x.isScene===!0&&x.onAfterRender(b,x,R),vt.resetDefaultState(),M=-1,_=null,E.pop(),E.length>0?(p=E[E.length-1],re===!0&&ne.setGlobalState(b.clippingPlanes,p.state.camera)):p=null,S.pop(),S.length>0?m=S[S.length-1]:m=null};function Ou(x,R,V,B){if(x.visible===!1)return;if(x.layers.test(R.layers)){if(x.isGroup)V=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(R);else if(x.isLight)p.pushLight(x),x.castShadow&&p.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||X.intersectsSprite(x)){B&&Xe.setFromMatrixPosition(x.matrixWorld).applyMatrix4(Re);let fe=q.update(x),ye=x.material;ye.visible&&m.push(x,fe,ye,V,Xe.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||X.intersectsObject(x))){let fe=q.update(x),ye=x.material;if(B&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),Xe.copy(x.boundingSphere.center)):(fe.boundingSphere===null&&fe.computeBoundingSphere(),Xe.copy(fe.boundingSphere.center)),Xe.applyMatrix4(x.matrixWorld).applyMatrix4(Re)),Array.isArray(ye)){let _e=fe.groups;for(let Fe=0,Ve=_e.length;Fe<Ve;Fe++){let xe=_e[Fe],at=ye[xe.materialIndex];at&&at.visible&&m.push(x,fe,at,V,Xe.z,xe)}}else ye.visible&&m.push(x,fe,ye,V,Xe.z,null)}}let ie=x.children;for(let fe=0,ye=ie.length;fe<ye;fe++)Ou(ie[fe],R,V,B)}function Lm(x,R,V,B){let N=x.opaque,ie=x.transmissive,fe=x.transparent;p.setupLightsView(V),re===!0&&ne.setGlobalState(b.clippingPlanes,V),B&&Ae.viewport(D.copy(B)),N.length>0&&Pa(N,R,V),ie.length>0&&Pa(ie,R,V),fe.length>0&&Pa(fe,R,V),Ae.buffers.depth.setTest(!0),Ae.buffers.depth.setMask(!0),Ae.buffers.color.setMask(!0),Ae.setPolygonOffset(!1)}function km(x,R,V,B){if((V.isScene===!0?V.overrideMaterial:null)!==null)return;p.state.transmissionRenderTarget[B.id]===void 0&&(p.state.transmissionRenderTarget[B.id]=new Wi(1,1,{generateMipmaps:!0,type:et.has("EXT_color_buffer_half_float")||et.has("EXT_color_buffer_float")?Sa:Gi,minFilter:os,samples:4,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:ot.workingColorSpace}));let ie=p.state.transmissionRenderTarget[B.id],fe=B.viewport||D;ie.setSize(fe.z,fe.w);let ye=b.getRenderTarget();b.setRenderTarget(ie),b.getClearColor($),K=b.getClearAlpha(),K<1&&b.setClearColor(16777215,.5),b.clear(),rt&&Ie.render(V);let _e=b.toneMapping;b.toneMapping=fr;let Fe=B.viewport;if(B.viewport!==void 0&&(B.viewport=void 0),p.setupLightsView(B),re===!0&&ne.setGlobalState(b.clippingPlanes,B),Pa(x,V,B),w.updateMultisampleRenderTarget(ie),w.updateRenderTargetMipmap(ie),et.has("WEBGL_multisampled_render_to_texture")===!1){let Ve=!1;for(let xe=0,at=R.length;xe<at;xe++){let yt=R[xe],bt=yt.object,cn=yt.geometry,lt=yt.material,Ee=yt.group;if(lt.side===Ui&&bt.layers.test(B.layers)){let pi=lt.side;lt.side=bn,lt.needsUpdate=!0,Um(bt,V,B,cn,lt,Ee),lt.side=pi,lt.needsUpdate=!0,Ve=!0}}Ve===!0&&(w.updateMultisampleRenderTarget(ie),w.updateRenderTargetMipmap(ie))}b.setRenderTarget(ye),b.setClearColor($,K),Fe!==void 0&&(B.viewport=Fe),b.toneMapping=_e}function Pa(x,R,V){let B=R.isScene===!0?R.overrideMaterial:null;for(let N=0,ie=x.length;N<ie;N++){let fe=x[N],ye=fe.object,_e=fe.geometry,Fe=B===null?fe.material:B,Ve=fe.group;ye.layers.test(V.layers)&&Um(ye,R,V,_e,Fe,Ve)}}function Um(x,R,V,B,N,ie){x.onBeforeRender(b,R,V,B,N,ie),x.modelViewMatrix.multiplyMatrices(V.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),N.onBeforeRender(b,R,V,B,x,ie),N.transparent===!0&&N.side===Ui&&N.forceSinglePass===!1?(N.side=bn,N.needsUpdate=!0,b.renderBufferDirect(V,R,B,N,x,ie),N.side=hr,N.needsUpdate=!0,b.renderBufferDirect(V,R,B,N,x,ie),N.side=Ui):b.renderBufferDirect(V,R,B,N,x,ie),x.onAfterRender(b,R,V,B,N,ie)}function Oa(x,R,V){R.isScene!==!0&&(R=wt);let B=Te.get(x),N=p.state.lights,ie=p.state.shadowsArray,fe=N.state.version,ye=Me.getParameters(x,N.state,ie,R,V),_e=Me.getProgramCacheKey(ye),Fe=B.programs;B.environment=x.isMeshStandardMaterial?R.environment:null,B.fog=R.fog,B.envMap=(x.isMeshStandardMaterial?k:y).get(x.envMap||B.environment),B.envMapRotation=B.environment!==null&&x.envMap===null?R.environmentRotation:x.envMapRotation,Fe===void 0&&(x.addEventListener("dispose",Ue),Fe=new Map,B.programs=Fe);let Ve=Fe.get(_e);if(Ve!==void 0){if(B.currentProgram===Ve&&B.lightsStateVersion===fe)return Bm(x,ye),Ve}else ye.uniforms=Me.getUniforms(x),x.onBeforeCompile(ye,b),Ve=Me.acquireProgram(ye,_e),Fe.set(_e,Ve),B.uniforms=ye.uniforms;let xe=B.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(xe.clippingPlanes=ne.uniform),Bm(x,ye),B.needsLights=Jx(x),B.lightsStateVersion=fe,B.needsLights&&(xe.ambientLightColor.value=N.state.ambient,xe.lightProbe.value=N.state.probe,xe.directionalLights.value=N.state.directional,xe.directionalLightShadows.value=N.state.directionalShadow,xe.spotLights.value=N.state.spot,xe.spotLightShadows.value=N.state.spotShadow,xe.rectAreaLights.value=N.state.rectArea,xe.ltc_1.value=N.state.rectAreaLTC1,xe.ltc_2.value=N.state.rectAreaLTC2,xe.pointLights.value=N.state.point,xe.pointLightShadows.value=N.state.pointShadow,xe.hemisphereLights.value=N.state.hemi,xe.directionalShadowMap.value=N.state.directionalShadowMap,xe.directionalShadowMatrix.value=N.state.directionalShadowMatrix,xe.spotShadowMap.value=N.state.spotShadowMap,xe.spotLightMatrix.value=N.state.spotLightMatrix,xe.spotLightMap.value=N.state.spotLightMap,xe.pointShadowMap.value=N.state.pointShadowMap,xe.pointShadowMatrix.value=N.state.pointShadowMatrix),B.currentProgram=Ve,B.uniformsList=null,Ve}function Vm(x){if(x.uniformsList===null){let R=x.currentProgram.getUniforms();x.uniformsList=so.seqWithValue(R.seq,x.uniforms)}return x.uniformsList}function Bm(x,R){let V=Te.get(x);V.outputColorSpace=R.outputColorSpace,V.batching=R.batching,V.batchingColor=R.batchingColor,V.instancing=R.instancing,V.instancingColor=R.instancingColor,V.instancingMorph=R.instancingMorph,V.skinning=R.skinning,V.morphTargets=R.morphTargets,V.morphNormals=R.morphNormals,V.morphColors=R.morphColors,V.morphTargetsCount=R.morphTargetsCount,V.numClippingPlanes=R.numClippingPlanes,V.numIntersection=R.numClipIntersection,V.vertexAlphas=R.vertexAlphas,V.vertexTangents=R.vertexTangents,V.toneMapping=R.toneMapping}function Yx(x,R,V,B,N){R.isScene!==!0&&(R=wt),w.resetTextureUnits();let ie=R.fog,fe=B.isMeshStandardMaterial?R.environment:null,ye=A===null?b.outputColorSpace:A.isXRRenderTarget===!0?A.texture.colorSpace:go,_e=(B.isMeshStandardMaterial?k:y).get(B.envMap||fe),Fe=B.vertexColors===!0&&!!V.attributes.color&&V.attributes.color.itemSize===4,Ve=!!V.attributes.tangent&&(!!B.normalMap||B.anisotropy>0),xe=!!V.morphAttributes.position,at=!!V.morphAttributes.normal,yt=!!V.morphAttributes.color,bt=fr;B.toneMapped&&(A===null||A.isXRRenderTarget===!0)&&(bt=b.toneMapping);let cn=V.morphAttributes.position||V.morphAttributes.normal||V.morphAttributes.color,lt=cn!==void 0?cn.length:0,Ee=Te.get(B),pi=p.state.lights;if(re===!0&&(be===!0||x!==_)){let Dn=x===_&&B.id===M;ne.setState(B,x,Dn)}let ct=!1;B.version===Ee.__version?(Ee.needsLights&&Ee.lightsStateVersion!==pi.state.version||Ee.outputColorSpace!==ye||N.isBatchedMesh&&Ee.batching===!1||!N.isBatchedMesh&&Ee.batching===!0||N.isBatchedMesh&&Ee.batchingColor===!0&&N.colorTexture===null||N.isBatchedMesh&&Ee.batchingColor===!1&&N.colorTexture!==null||N.isInstancedMesh&&Ee.instancing===!1||!N.isInstancedMesh&&Ee.instancing===!0||N.isSkinnedMesh&&Ee.skinning===!1||!N.isSkinnedMesh&&Ee.skinning===!0||N.isInstancedMesh&&Ee.instancingColor===!0&&N.instanceColor===null||N.isInstancedMesh&&Ee.instancingColor===!1&&N.instanceColor!==null||N.isInstancedMesh&&Ee.instancingMorph===!0&&N.morphTexture===null||N.isInstancedMesh&&Ee.instancingMorph===!1&&N.morphTexture!==null||Ee.envMap!==_e||B.fog===!0&&Ee.fog!==ie||Ee.numClippingPlanes!==void 0&&(Ee.numClippingPlanes!==ne.numPlanes||Ee.numIntersection!==ne.numIntersection)||Ee.vertexAlphas!==Fe||Ee.vertexTangents!==Ve||Ee.morphTargets!==xe||Ee.morphNormals!==at||Ee.morphColors!==yt||Ee.toneMapping!==bt||Ee.morphTargetsCount!==lt)&&(ct=!0):(ct=!0,Ee.__version=B.version);let Hn=Ee.currentProgram;ct===!0&&(Hn=Oa(B,R,N));let gs=!1,Mn=!1,Eo=!1,Mt=Hn.getUniforms(),ti=Ee.uniforms;if(Ae.useProgram(Hn.program)&&(gs=!0,Mn=!0,Eo=!0),B.id!==M&&(M=B.id,Mn=!0),gs||_!==x){Ae.buffers.depth.getReversed()?(ce.copy(x.projectionMatrix),LD(ce),kD(ce),Mt.setValue(P,"projectionMatrix",ce)):Mt.setValue(P,"projectionMatrix",x.projectionMatrix),Mt.setValue(P,"viewMatrix",x.matrixWorldInverse);let $i=Mt.map.cameraPosition;$i!==void 0&&$i.setValue(P,ke.setFromMatrixPosition(x.matrixWorld)),tt.logarithmicDepthBuffer&&Mt.setValue(P,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(B.isMeshPhongMaterial||B.isMeshToonMaterial||B.isMeshLambertMaterial||B.isMeshBasicMaterial||B.isMeshStandardMaterial||B.isShaderMaterial)&&Mt.setValue(P,"isOrthographic",x.isOrthographicCamera===!0),_!==x&&(_=x,Mn=!0,Eo=!0)}if(N.isSkinnedMesh){Mt.setOptional(P,N,"bindMatrix"),Mt.setOptional(P,N,"bindMatrixInverse");let Dn=N.skeleton;Dn&&(Dn.boneTexture===null&&Dn.computeBoneTexture(),Mt.setValue(P,"boneTexture",Dn.boneTexture,w))}N.isBatchedMesh&&(Mt.setOptional(P,N,"batchingTexture"),Mt.setValue(P,"batchingTexture",N._matricesTexture,w),Mt.setOptional(P,N,"batchingIdTexture"),Mt.setValue(P,"batchingIdTexture",N._indirectTexture,w),Mt.setOptional(P,N,"batchingColorTexture"),N._colorsTexture!==null&&Mt.setValue(P,"batchingColorTexture",N._colorsTexture,w));let So=V.morphAttributes;if((So.position!==void 0||So.normal!==void 0||So.color!==void 0)&&Pe.update(N,V,Hn),(Mn||Ee.receiveShadow!==N.receiveShadow)&&(Ee.receiveShadow=N.receiveShadow,Mt.setValue(P,"receiveShadow",N.receiveShadow)),B.isMeshGouraudMaterial&&B.envMap!==null&&(ti.envMap.value=_e,ti.flipEnvMap.value=_e.isCubeTexture&&_e.isRenderTargetTexture===!1?-1:1),B.isMeshStandardMaterial&&B.envMap===null&&R.environment!==null&&(ti.envMapIntensity.value=R.environmentIntensity),Mn&&(Mt.setValue(P,"toneMappingExposure",b.toneMappingExposure),Ee.needsLights&&Zx(ti,Eo),ie&&B.fog===!0&&ue.refreshFogUniforms(ti,ie),ue.refreshMaterialUniforms(ti,B,W,te,p.state.transmissionRenderTarget[x.id]),so.upload(P,Vm(Ee),ti,w)),B.isShaderMaterial&&B.uniformsNeedUpdate===!0&&(so.upload(P,Vm(Ee),ti,w),B.uniformsNeedUpdate=!1),B.isSpriteMaterial&&Mt.setValue(P,"center",N.center),Mt.setValue(P,"modelViewMatrix",N.modelViewMatrix),Mt.setValue(P,"normalMatrix",N.normalMatrix),Mt.setValue(P,"modelMatrix",N.matrixWorld),B.isShaderMaterial||B.isRawShaderMaterial){let Dn=B.uniformsGroups;for(let $i=0,qi=Dn.length;$i<qi;$i++){let Hm=Dn[$i];I.update(Hm,Hn),I.bind(Hm,Hn)}}return Hn}function Zx(x,R){x.ambientLightColor.needsUpdate=R,x.lightProbe.needsUpdate=R,x.directionalLights.needsUpdate=R,x.directionalLightShadows.needsUpdate=R,x.pointLights.needsUpdate=R,x.pointLightShadows.needsUpdate=R,x.spotLights.needsUpdate=R,x.spotLightShadows.needsUpdate=R,x.rectAreaLights.needsUpdate=R,x.hemisphereLights.needsUpdate=R}function Jx(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return T},this.getActiveMipmapLevel=function(){return C},this.getRenderTarget=function(){return A},this.setRenderTargetTextures=function(x,R,V){Te.get(x.texture).__webglTexture=R,Te.get(x.depthTexture).__webglTexture=V;let B=Te.get(x);B.__hasExternalTextures=!0,B.__autoAllocateDepthBuffer=V===void 0,B.__autoAllocateDepthBuffer||et.has("WEBGL_multisampled_render_to_texture")===!0&&(console.warn("THREE.WebGLRenderer: Render-to-texture extension was disabled because an external texture was provided"),B.__useRenderToTexture=!1)},this.setRenderTargetFramebuffer=function(x,R){let V=Te.get(x);V.__webglFramebuffer=R,V.__useDefaultFramebuffer=R===void 0},this.setRenderTarget=function(x,R=0,V=0){A=x,T=R,C=V;let B=!0,N=null,ie=!1,fe=!1;if(x){let _e=Te.get(x);if(_e.__useDefaultFramebuffer!==void 0)Ae.bindFramebuffer(P.FRAMEBUFFER,null),B=!1;else if(_e.__webglFramebuffer===void 0)w.setupRenderTarget(x);else if(_e.__hasExternalTextures)w.rebindTextures(x,Te.get(x.texture).__webglTexture,Te.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let xe=x.depthTexture;if(_e.__boundDepthTexture!==xe){if(xe!==null&&Te.has(xe)&&(x.width!==xe.image.width||x.height!==xe.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");w.setupDepthRenderbuffer(x)}}let Fe=x.texture;(Fe.isData3DTexture||Fe.isDataArrayTexture||Fe.isCompressedArrayTexture)&&(fe=!0);let Ve=Te.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(Ve[R])?N=Ve[R][V]:N=Ve[R],ie=!0):x.samples>0&&w.useMultisampledRTT(x)===!1?N=Te.get(x).__webglMultisampledFramebuffer:Array.isArray(Ve)?N=Ve[V]:N=Ve,D.copy(x.viewport),z.copy(x.scissor),H=x.scissorTest}else D.copy(Ce).multiplyScalar(W).floor(),z.copy(qe).multiplyScalar(W).floor(),H=gt;if(Ae.bindFramebuffer(P.FRAMEBUFFER,N)&&B&&Ae.drawBuffers(x,N),Ae.viewport(D),Ae.scissor(z),Ae.setScissorTest(H),ie){let _e=Te.get(x.texture);P.framebufferTexture2D(P.FRAMEBUFFER,P.COLOR_ATTACHMENT0,P.TEXTURE_CUBE_MAP_POSITIVE_X+R,_e.__webglTexture,V)}else if(fe){let _e=Te.get(x.texture),Fe=R||0;P.framebufferTextureLayer(P.FRAMEBUFFER,P.COLOR_ATTACHMENT0,_e.__webglTexture,V||0,Fe)}M=-1},this.readRenderTargetPixels=function(x,R,V,B,N,ie,fe){if(!(x&&x.isWebGLRenderTarget)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let ye=Te.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&fe!==void 0&&(ye=ye[fe]),ye){Ae.bindFramebuffer(P.FRAMEBUFFER,ye);try{let _e=x.texture,Fe=_e.format,Ve=_e.type;if(!tt.textureFormatReadable(Fe)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!tt.textureTypeReadable(Ve)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}R>=0&&R<=x.width-B&&V>=0&&V<=x.height-N&&P.readPixels(R,V,B,N,He.convert(Fe),He.convert(Ve),ie)}finally{let _e=A!==null?Te.get(A).__webglFramebuffer:null;Ae.bindFramebuffer(P.FRAMEBUFFER,_e)}}},this.readRenderTargetPixelsAsync=function(x,R,V,B,N,ie,fe){return vs(this,null,function*(){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let ye=Te.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&fe!==void 0&&(ye=ye[fe]),ye){let _e=x.texture,Fe=_e.format,Ve=_e.type;if(!tt.textureFormatReadable(Fe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!tt.textureTypeReadable(Ve))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");if(R>=0&&R<=x.width-B&&V>=0&&V<=x.height-N){Ae.bindFramebuffer(P.FRAMEBUFFER,ye);let xe=P.createBuffer();P.bindBuffer(P.PIXEL_PACK_BUFFER,xe),P.bufferData(P.PIXEL_PACK_BUFFER,ie.byteLength,P.STREAM_READ),P.readPixels(R,V,B,N,He.convert(Fe),He.convert(Ve),0);let at=A!==null?Te.get(A).__webglFramebuffer:null;Ae.bindFramebuffer(P.FRAMEBUFFER,at);let yt=P.fenceSync(P.SYNC_GPU_COMMANDS_COMPLETE,0);return P.flush(),yield FD(P,yt,4),P.bindBuffer(P.PIXEL_PACK_BUFFER,xe),P.getBufferSubData(P.PIXEL_PACK_BUFFER,0,ie),P.deleteBuffer(xe),P.deleteSync(yt),ie}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")}})},this.copyFramebufferToTexture=function(x,R=null,V=0){x.isTexture!==!0&&(ga("WebGLRenderer: copyFramebufferToTexture function signature has changed."),R=arguments[0]||null,x=arguments[1]);let B=Math.pow(2,-V),N=Math.floor(x.image.width*B),ie=Math.floor(x.image.height*B),fe=R!==null?R.x:0,ye=R!==null?R.y:0;w.setTexture2D(x,0),P.copyTexSubImage2D(P.TEXTURE_2D,V,0,0,fe,ye,N,ie),Ae.unbindTexture()},this.copyTextureToTexture=function(x,R,V=null,B=null,N=0){x.isTexture!==!0&&(ga("WebGLRenderer: copyTextureToTexture function signature has changed."),B=arguments[0]||null,x=arguments[1],R=arguments[2],N=arguments[3]||0,V=null);let ie,fe,ye,_e,Fe,Ve,xe,at,yt,bt=x.isCompressedTexture?x.mipmaps[N]:x.image;V!==null?(ie=V.max.x-V.min.x,fe=V.max.y-V.min.y,ye=V.isBox3?V.max.z-V.min.z:1,_e=V.min.x,Fe=V.min.y,Ve=V.isBox3?V.min.z:0):(ie=bt.width,fe=bt.height,ye=bt.depth||1,_e=0,Fe=0,Ve=0),B!==null?(xe=B.x,at=B.y,yt=B.z):(xe=0,at=0,yt=0);let cn=He.convert(R.format),lt=He.convert(R.type),Ee;R.isData3DTexture?(w.setTexture3D(R,0),Ee=P.TEXTURE_3D):R.isDataArrayTexture||R.isCompressedArrayTexture?(w.setTexture2DArray(R,0),Ee=P.TEXTURE_2D_ARRAY):(w.setTexture2D(R,0),Ee=P.TEXTURE_2D),P.pixelStorei(P.UNPACK_FLIP_Y_WEBGL,R.flipY),P.pixelStorei(P.UNPACK_PREMULTIPLY_ALPHA_WEBGL,R.premultiplyAlpha),P.pixelStorei(P.UNPACK_ALIGNMENT,R.unpackAlignment);let pi=P.getParameter(P.UNPACK_ROW_LENGTH),ct=P.getParameter(P.UNPACK_IMAGE_HEIGHT),Hn=P.getParameter(P.UNPACK_SKIP_PIXELS),gs=P.getParameter(P.UNPACK_SKIP_ROWS),Mn=P.getParameter(P.UNPACK_SKIP_IMAGES);P.pixelStorei(P.UNPACK_ROW_LENGTH,bt.width),P.pixelStorei(P.UNPACK_IMAGE_HEIGHT,bt.height),P.pixelStorei(P.UNPACK_SKIP_PIXELS,_e),P.pixelStorei(P.UNPACK_SKIP_ROWS,Fe),P.pixelStorei(P.UNPACK_SKIP_IMAGES,Ve);let Eo=x.isDataArrayTexture||x.isData3DTexture,Mt=R.isDataArrayTexture||R.isData3DTexture;if(x.isRenderTargetTexture||x.isDepthTexture){let ti=Te.get(x),So=Te.get(R),Dn=Te.get(ti.__renderTarget),$i=Te.get(So.__renderTarget);Ae.bindFramebuffer(P.READ_FRAMEBUFFER,Dn.__webglFramebuffer),Ae.bindFramebuffer(P.DRAW_FRAMEBUFFER,$i.__webglFramebuffer);for(let qi=0;qi<ye;qi++)Eo&&P.framebufferTextureLayer(P.READ_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Te.get(x).__webglTexture,N,Ve+qi),x.isDepthTexture?(Mt&&P.framebufferTextureLayer(P.DRAW_FRAMEBUFFER,P.COLOR_ATTACHMENT0,Te.get(R).__webglTexture,N,yt+qi),P.blitFramebuffer(_e,Fe,ie,fe,xe,at,ie,fe,P.DEPTH_BUFFER_BIT,P.NEAREST)):Mt?P.copyTexSubImage3D(Ee,N,xe,at,yt+qi,_e,Fe,ie,fe):P.copyTexSubImage2D(Ee,N,xe,at,yt+qi,_e,Fe,ie,fe);Ae.bindFramebuffer(P.READ_FRAMEBUFFER,null),Ae.bindFramebuffer(P.DRAW_FRAMEBUFFER,null)}else Mt?x.isDataTexture||x.isData3DTexture?P.texSubImage3D(Ee,N,xe,at,yt,ie,fe,ye,cn,lt,bt.data):R.isCompressedArrayTexture?P.compressedTexSubImage3D(Ee,N,xe,at,yt,ie,fe,ye,cn,bt.data):P.texSubImage3D(Ee,N,xe,at,yt,ie,fe,ye,cn,lt,bt):x.isDataTexture?P.texSubImage2D(P.TEXTURE_2D,N,xe,at,ie,fe,cn,lt,bt.data):x.isCompressedTexture?P.compressedTexSubImage2D(P.TEXTURE_2D,N,xe,at,bt.width,bt.height,cn,bt.data):P.texSubImage2D(P.TEXTURE_2D,N,xe,at,ie,fe,cn,lt,bt);P.pixelStorei(P.UNPACK_ROW_LENGTH,pi),P.pixelStorei(P.UNPACK_IMAGE_HEIGHT,ct),P.pixelStorei(P.UNPACK_SKIP_PIXELS,Hn),P.pixelStorei(P.UNPACK_SKIP_ROWS,gs),P.pixelStorei(P.UNPACK_SKIP_IMAGES,Mn),N===0&&R.generateMipmaps&&P.generateMipmap(Ee),Ae.unbindTexture()},this.copyTextureToTexture3D=function(x,R,V=null,B=null,N=0){return x.isTexture!==!0&&(ga("WebGLRenderer: copyTextureToTexture3D function signature has changed."),V=arguments[0]||null,B=arguments[1]||null,x=arguments[2],R=arguments[3],N=arguments[4]||0),ga('WebGLRenderer: copyTextureToTexture3D function has been deprecated. Use "copyTextureToTexture" instead.'),this.copyTextureToTexture(x,R,V,B,N)},this.initRenderTarget=function(x){Te.get(x).__webglFramebuffer===void 0&&w.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?w.setTextureCube(x,0):x.isData3DTexture?w.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?w.setTexture2DArray(x,0):w.setTexture2D(x,0),Ae.unbindTexture()},this.resetState=function(){T=0,C=0,A=null,Ae.reset(),vt.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Bi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorspace=ot._getDrawingBufferColorSpace(e),t.unpackColorSpace=ot._getUnpackColorSpace()}};var iu=class extends vr{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new cs,this.environmentIntensity=1,this.environmentRotation=new cs,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}};var Ma=class extends gr{static get type(){return"PointsMaterial"}constructor(e){super(),this.isPointsMaterial=!0,this.color=new $e(16777215),this.map=null,this.alphaMap=null,this.size=1,this.sizeAttenuation=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.size=e.size,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},ex=new At,rm=new qc,Oc=new fo,Fc=new O,ru=class extends vr{constructor(e=new Cn,t=new Ma){super(),this.isPoints=!0,this.type="Points",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Points.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),Oc.copy(i.boundingSphere),Oc.applyMatrix4(r),Oc.radius+=s,e.ray.intersectsSphere(Oc)===!1)return;ex.copy(r).invert(),rm.copy(e.ray).applyMatrix4(ex);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),l=a*a,c=i.index,d=i.attributes.position;if(c!==null){let h=Math.max(0,o.start),f=Math.min(c.count,o.start+o.count);for(let g=h,v=f;g<v;g++){let m=c.getX(g);Fc.fromBufferAttribute(d,m),tx(Fc,m,l,r,e,t,this)}}else{let h=Math.max(0,o.start),f=Math.min(d.count,o.start+o.count);for(let g=h,v=f;g<v;g++)Fc.fromBufferAttribute(d,g),tx(Fc,g,l,r,e,t,this)}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function tx(n,e,t,i,r,s,o){let a=rm.distanceSqToPoint(n);if(a<t){let l=new O;rm.closestPointToPoint(n,l),l.applyMatrix4(i);let c=r.ray.origin.distanceTo(l);if(c<r.near||c>r.far)return;s.push({distance:c,distanceToRay:Math.sqrt(a),point:l,index:e,face:null,faceIndex:null,barycoord:null,object:o})}}var sm=class n extends Cn{constructor(e=1,t=1,i=1,r=32,s=1,o=!1,a=0,l=Math.PI*2){super(),this.type="CylinderGeometry",this.parameters={radiusTop:e,radiusBottom:t,height:i,radialSegments:r,heightSegments:s,openEnded:o,thetaStart:a,thetaLength:l};let c=this;r=Math.floor(r),s=Math.floor(s);let u=[],d=[],h=[],f=[],g=0,v=[],m=i/2,p=0;S(),o===!1&&(e>0&&E(!0),t>0&&E(!1)),this.setIndex(u),this.setAttribute("position",new Yt(d,3)),this.setAttribute("normal",new Yt(h,3)),this.setAttribute("uv",new Yt(f,2));function S(){let b=new O,F=new O,T=0,C=(t-e)/i;for(let A=0;A<=s;A++){let M=[],_=A/s,D=_*(t-e)+e;for(let z=0;z<=r;z++){let H=z/r,$=H*l+a,K=Math.sin($),j=Math.cos($);F.x=D*K,F.y=-_*i+m,F.z=D*j,d.push(F.x,F.y,F.z),b.set(K,C,j).normalize(),h.push(b.x,b.y,b.z),f.push(H,1-_),M.push(g++)}v.push(M)}for(let A=0;A<r;A++)for(let M=0;M<s;M++){let _=v[M][A],D=v[M+1][A],z=v[M+1][A+1],H=v[M][A+1];(e>0||M!==0)&&(u.push(_,D,H),T+=3),(t>0||M!==s-1)&&(u.push(D,z,H),T+=3)}c.addGroup(p,T,0),p+=T}function E(b){let F=g,T=new Qe,C=new O,A=0,M=b===!0?e:t,_=b===!0?1:-1;for(let z=1;z<=r;z++)d.push(0,m*_,0),h.push(0,_,0),f.push(.5,.5),g++;let D=g;for(let z=0;z<=r;z++){let $=z/r*l+a,K=Math.cos($),j=Math.sin($);C.x=M*j,C.y=m*_,C.z=M*K,d.push(C.x,C.y,C.z),h.push(0,_,0),T.x=K*.5+.5,T.y=j*.5*_+.5,f.push(T.x,T.y),g++}for(let z=0;z<r;z++){let H=F+z,$=D+z;b===!0?u.push($,$+1,H):u.push($+1,$,H),A+=3}c.addGroup(p,A,b===!0?1:2),p+=A}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radiusTop,e.radiusBottom,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}},su=class n extends sm{constructor(e=1,t=1,i=32,r=1,s=!1,o=0,a=Math.PI*2){super(0,e,t,i,r,s,o,a),this.type="ConeGeometry",this.parameters={radius:e,height:t,radialSegments:i,heightSegments:r,openEnded:s,thetaStart:o,thetaLength:a}}static fromJSON(e){return new n(e.radius,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}},ou=class n extends Cn{constructor(e=[],t=[],i=1,r=0){super(),this.type="PolyhedronGeometry",this.parameters={vertices:e,indices:t,radius:i,detail:r};let s=[],o=[];a(r),c(i),u(),this.setAttribute("position",new Yt(s,3)),this.setAttribute("normal",new Yt(s.slice(),3)),this.setAttribute("uv",new Yt(o,2)),r===0?this.computeVertexNormals():this.normalizeNormals();function a(S){let E=new O,b=new O,F=new O;for(let T=0;T<t.length;T+=3)f(t[T+0],E),f(t[T+1],b),f(t[T+2],F),l(E,b,F,S)}function l(S,E,b,F){let T=F+1,C=[];for(let A=0;A<=T;A++){C[A]=[];let M=S.clone().lerp(b,A/T),_=E.clone().lerp(b,A/T),D=T-A;for(let z=0;z<=D;z++)z===0&&A===T?C[A][z]=M:C[A][z]=M.clone().lerp(_,z/D)}for(let A=0;A<T;A++)for(let M=0;M<2*(T-A)-1;M++){let _=Math.floor(M/2);M%2===0?(h(C[A][_+1]),h(C[A+1][_]),h(C[A][_])):(h(C[A][_+1]),h(C[A+1][_+1]),h(C[A+1][_]))}}function c(S){let E=new O;for(let b=0;b<s.length;b+=3)E.x=s[b+0],E.y=s[b+1],E.z=s[b+2],E.normalize().multiplyScalar(S),s[b+0]=E.x,s[b+1]=E.y,s[b+2]=E.z}function u(){let S=new O;for(let E=0;E<s.length;E+=3){S.x=s[E+0],S.y=s[E+1],S.z=s[E+2];let b=m(S)/2/Math.PI+.5,F=p(S)/Math.PI+.5;o.push(b,1-F)}g(),d()}function d(){for(let S=0;S<o.length;S+=6){let E=o[S+0],b=o[S+2],F=o[S+4],T=Math.max(E,b,F),C=Math.min(E,b,F);T>.9&&C<.1&&(E<.2&&(o[S+0]+=1),b<.2&&(o[S+2]+=1),F<.2&&(o[S+4]+=1))}}function h(S){s.push(S.x,S.y,S.z)}function f(S,E){let b=S*3;E.x=e[b+0],E.y=e[b+1],E.z=e[b+2]}function g(){let S=new O,E=new O,b=new O,F=new O,T=new Qe,C=new Qe,A=new Qe;for(let M=0,_=0;M<s.length;M+=9,_+=6){S.set(s[M+0],s[M+1],s[M+2]),E.set(s[M+3],s[M+4],s[M+5]),b.set(s[M+6],s[M+7],s[M+8]),T.set(o[_+0],o[_+1]),C.set(o[_+2],o[_+3]),A.set(o[_+4],o[_+5]),F.copy(S).add(E).add(b).divideScalar(3);let D=m(F);v(T,_+0,S,D),v(C,_+2,E,D),v(A,_+4,b,D)}}function v(S,E,b,F){F<0&&S.x===1&&(o[E]=S.x-1),b.x===0&&b.z===0&&(o[E]=F/2/Math.PI+.5)}function m(S){return Math.atan2(S.z,-S.x)}function p(S){return Math.atan2(-S.y,Math.sqrt(S.x*S.x+S.z*S.z))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.vertices,e.indices,e.radius,e.details)}};var au=class n extends ou{constructor(e=1,t=0){let i=(1+Math.sqrt(5))/2,r=[-1,i,0,1,i,0,-1,-i,0,1,-i,0,0,-1,i,0,1,i,0,-1,-i,0,1,-i,i,0,-1,i,0,1,-i,0,-1,-i,0,1],s=[0,11,5,0,5,1,0,1,7,0,7,10,0,10,11,1,5,9,5,11,4,11,10,2,10,7,6,7,1,8,3,9,4,3,4,2,3,2,6,3,6,8,3,8,9,4,9,5,2,4,11,6,2,10,8,6,7,9,8,1];super(r,s,e,t),this.type="IcosahedronGeometry",this.parameters={radius:e,detail:t}}static fromJSON(e){return new n(e.radius,e.detail)}},lu=class n extends ou{constructor(e=1,t=0){let i=[1,0,0,-1,0,0,0,1,0,0,-1,0,0,0,1,0,0,-1],r=[0,2,4,0,4,3,0,3,5,0,5,2,1,2,5,1,5,3,1,3,4,1,4,2];super(i,r,e,t),this.type="OctahedronGeometry",this.parameters={radius:e,detail:t}}static fromJSON(e){return new n(e.radius,e.detail)}};var po=class n extends Cn{constructor(e=1,t=.4,i=12,r=48,s=Math.PI*2){super(),this.type="TorusGeometry",this.parameters={radius:e,tube:t,radialSegments:i,tubularSegments:r,arc:s},i=Math.floor(i),r=Math.floor(r);let o=[],a=[],l=[],c=[],u=new O,d=new O,h=new O;for(let f=0;f<=i;f++)for(let g=0;g<=r;g++){let v=g/r*s,m=f/i*Math.PI*2;d.x=(e+t*Math.cos(m))*Math.cos(v),d.y=(e+t*Math.cos(m))*Math.sin(v),d.z=t*Math.sin(m),a.push(d.x,d.y,d.z),u.x=e*Math.cos(v),u.y=e*Math.sin(v),h.subVectors(d,u).normalize(),l.push(h.x,h.y,h.z),c.push(g/r),c.push(f/i)}for(let f=1;f<=i;f++)for(let g=1;g<=r;g++){let v=(r+1)*f+g-1,m=(r+1)*(f-1)+g-1,p=(r+1)*(f-1)+g,S=(r+1)*f+g;o.push(v,m,S),o.push(m,p,S)}this.setIndex(o),this.setAttribute("position",new Yt(a,3)),this.setAttribute("normal",new Yt(l,3)),this.setAttribute("uv",new Yt(c,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.tube,e.radialSegments,e.tubularSegments,e.arc)}};var ds=class extends gr{static get type(){return"MeshStandardMaterial"}constructor(e){super(),this.isMeshStandardMaterial=!0,this.defines={STANDARD:""},this.color=new $e(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new $e(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=yx,this.normalScale=new Qe(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new cs,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};function Lc(n,e,t){return!n||!t&&n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function RN(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}var mo=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let l=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===l)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},om=class extends mo{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:r0,endingEnd:r0}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],l=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case s0:s=e,a=2*t-i;break;case o0:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(l===void 0)switch(this.getSettings_().endingEnd){case s0:o=e,l=2*i-t;break;case o0:o=1,l=i+r[1]-r[0];break;default:o=e-1,l=t}let c=(i-t)*.5,u=this.valueSize;this._weightPrev=c/(t-a),this._weightNext=c/(l-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=this._offsetPrev,d=this._offsetNext,h=this._weightPrev,f=this._weightNext,g=(i-t)/(r-t),v=g*g,m=v*g,p=-h*m+2*h*v-h*g,S=(1+h)*m+(-1.5-2*h)*v+(-.5+h)*g+1,E=(-1-f)*m+(1.5+f)*v+.5*g,b=f*m-f*v;for(let F=0;F!==a;++F)s[F]=p*o[u+F]+S*o[c+F]+E*o[l+F]+b*o[d+F];return s}},am=class extends mo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=(i-t)/(r-t),d=1-u;for(let h=0;h!==a;++h)s[h]=o[c+h]*d+o[l+h]*u;return s}},lm=class extends mo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},ei=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Lc(t,this.TimeBufferType),this.values=Lc(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Lc(e.times,Array),values:Lc(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new lm(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new am(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new om(this.times,this.values,this.getValueSize(),e)}setInterpolation(e){let t;switch(e){case zc:t=this.InterpolantFactoryMethodDiscrete;break;case Vp:t=this.InterpolantFactoryMethodLinear;break;case Eh:t=this.InterpolantFactoryMethodSmooth;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return console.warn("THREE.KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return zc;case this.InterpolantFactoryMethodLinear:return Vp;case this.InterpolantFactoryMethodSmooth:return Eh}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(console.error("THREE.KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(console.error("THREE.KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let l=i[a];if(typeof l=="number"&&isNaN(l)){console.error("THREE.KeyframeTrack: Time is not a valid number.",this,a,l),e=!1;break}if(o!==null&&o>l){console.error("THREE.KeyframeTrack: Out of order keys.",this,a,l,o),e=!1;break}o=l}if(r!==void 0&&RN(r))for(let a=0,l=r.length;a!==l;++a){let c=r[a];if(isNaN(c)){console.error("THREE.KeyframeTrack: Value is not a valid number.",this,a,c),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Eh,s=e.length-1,o=1;for(let a=1;a<s;++a){let l=!1,c=e[a],u=e[a+1];if(c!==u&&(a!==1||c!==e[0]))if(r)l=!0;else{let d=a*i,h=d-i,f=d+i;for(let g=0;g!==i;++g){let v=t[d+g];if(v!==t[h+g]||v!==t[f+g]){l=!0;break}}}if(l){if(a!==o){e[o]=e[a];let d=a*i,h=o*i;for(let f=0;f!==i;++f)t[h+f]=t[d+f]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,l=o*i,c=0;c!==i;++c)t[l+c]=t[a+c];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};ei.prototype.TimeBufferType=Float32Array;ei.prototype.ValueBufferType=Float32Array;ei.prototype.DefaultInterpolation=Vp;var fs=class extends ei{constructor(e,t,i){super(e,t,i)}};fs.prototype.ValueTypeName="bool";fs.prototype.ValueBufferType=Array;fs.prototype.DefaultInterpolation=zc;fs.prototype.InterpolantFactoryMethodLinear=void 0;fs.prototype.InterpolantFactoryMethodSmooth=void 0;var cm=class extends ei{};cm.prototype.ValueTypeName="color";var um=class extends ei{};um.prototype.ValueTypeName="number";var dm=class extends mo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=(i-t)/(r-t),c=e*a;for(let u=c+a;c!==u;c+=4)mr.slerpFlat(s,0,o,c-a,o,c,l);return s}},cu=class extends ei{InterpolantFactoryMethodLinear(e){return new dm(this.times,this.values,this.getValueSize(),e)}};cu.prototype.ValueTypeName="quaternion";cu.prototype.InterpolantFactoryMethodSmooth=void 0;var hs=class extends ei{constructor(e,t,i){super(e,t,i)}};hs.prototype.ValueTypeName="string";hs.prototype.ValueBufferType=Array;hs.prototype.DefaultInterpolation=zc;hs.prototype.InterpolantFactoryMethodLinear=void 0;hs.prototype.InterpolantFactoryMethodSmooth=void 0;var fm=class extends ei{};fm.prototype.ValueTypeName="vector";var uu=class extends vr{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new $e(e),this.intensity=t}dispose(){}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,this.groundColor!==void 0&&(t.object.groundColor=this.groundColor.getHex()),this.distance!==void 0&&(t.object.distance=this.distance),this.angle!==void 0&&(t.object.angle=this.angle),this.decay!==void 0&&(t.object.decay=this.decay),this.penumbra!==void 0&&(t.object.penumbra=this.penumbra),this.shadow!==void 0&&(t.object.shadow=this.shadow.toJSON()),this.target!==void 0&&(t.object.target=this.target.uuid),t}};var Kh=new At,nx=new O,ix=new O,hm=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new Qe(512,512),this.map=null,this.mapPass=null,this.matrix=new At,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new ba,this._frameExtents=new Qe(1,1),this._viewportCount=1,this._viewports=[new mt(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;nx.setFromMatrixPosition(e.matrixWorld),t.position.copy(nx),ix.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(ix),t.updateMatrixWorld(),Kh.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Kh),i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Kh)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.mapSize.copy(e.mapSize),this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}};var rx=new At,ma=new O,Qh=new O,pm=class extends hm{constructor(){super(new nn(90,1,.5,500)),this.isPointLightShadow=!0,this._frameExtents=new Qe(4,2),this._viewportCount=6,this._viewports=[new mt(2,1,1,1),new mt(0,1,1,1),new mt(3,1,1,1),new mt(1,1,1,1),new mt(3,0,1,1),new mt(1,0,1,1)],this._cubeDirections=[new O(1,0,0),new O(-1,0,0),new O(0,0,1),new O(0,0,-1),new O(0,1,0),new O(0,-1,0)],this._cubeUps=[new O(0,1,0),new O(0,1,0),new O(0,1,0),new O(0,1,0),new O(0,0,1),new O(0,0,-1)]}updateMatrices(e,t=0){let i=this.camera,r=this.matrix,s=e.distance||i.far;s!==i.far&&(i.far=s,i.updateProjectionMatrix()),ma.setFromMatrixPosition(e.matrixWorld),i.position.copy(ma),Qh.copy(i.position),Qh.add(this._cubeDirections[t]),i.up.copy(this._cubeUps[t]),i.lookAt(Qh),i.updateMatrixWorld(),r.makeTranslation(-ma.x,-ma.y,-ma.z),rx.multiplyMatrices(i.projectionMatrix,i.matrixWorldInverse),this._frustum.setFromProjectionMatrix(rx)}},Ea=class extends uu{constructor(e,t,i=0,r=2){super(e,t),this.isPointLight=!0,this.type="PointLight",this.distance=i,this.decay=r,this.shadow=new pm}get power(){return this.intensity*4*Math.PI}set power(e){this.intensity=e/(4*Math.PI)}dispose(){this.shadow.dispose()}copy(e,t){return super.copy(e,t),this.distance=e.distance,this.decay=e.decay,this.shadow=e.shadow.clone(),this}};var du=class extends uu{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var fu=class{constructor(e=!0){this.autoStart=e,this.startTime=0,this.oldTime=0,this.elapsedTime=0,this.running=!1}start(){this.startTime=sx(),this.oldTime=this.startTime,this.elapsedTime=0,this.running=!0}stop(){this.getElapsedTime(),this.running=!1,this.autoStart=!1}getElapsedTime(){return this.getDelta(),this.elapsedTime}getDelta(){let e=0;if(this.autoStart&&!this.running)return this.start(),0;if(this.running){let t=sx();e=(t-this.oldTime)/1e3,this.oldTime=t,this.elapsedTime+=e}return e}};function sx(){return performance.now()}var Em="\\[\\]\\.:\\/",NN=new RegExp("["+Em+"]","g"),Sm="[^"+Em+"]",PN="[^"+Em.replace("\\.","")+"]",ON=/((?:WC+[\/:])*)/.source.replace("WC",Sm),FN=/(WCOD+)?/.source.replace("WCOD",PN),LN=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Sm),kN=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Sm),UN=new RegExp("^"+ON+FN+LN+kN+"$"),VN=["material","materials","bones","map"],mm=class{constructor(e,t,i){let r=i||Ut.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Ut=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(NN,"")}static parseTrackName(t){let i=UN.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);VN.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let l=o[a];if(l.name===i||l.uuid===i)return l;let c=r(l.children);if(c)return c}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){console.warn("THREE.PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){console.error("THREE.PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){console.error("THREE.PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){console.error("THREE.PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){console.error("THREE.PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){console.error("THREE.PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;console.error("THREE.PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let l=this.Versioning.None;this.targetObject=t,t.needsUpdate!==void 0?l=this.Versioning.NeedsUpdate:t.matrixWorldNeedsUpdate!==void 0&&(l=this.Versioning.MatrixWorldNeedsUpdate);let c=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}c=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(c=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(c=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[c],this.setValue=this.SetterByBindingTypeAndVersioning[c][l]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=mm,n})();Ut.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Ut.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Ut.prototype.GetterByBindingType=[Ut.prototype._getValue_direct,Ut.prototype._getValue_array,Ut.prototype._getValue_arrayElement,Ut.prototype._getValue_toArray];Ut.prototype.SetterByBindingTypeAndVersioning=[[Ut.prototype._setValue_direct,Ut.prototype._setValue_direct_setNeedsUpdate,Ut.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Ut.prototype._setValue_array,Ut.prototype._setValue_array_setNeedsUpdate,Ut.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Ut.prototype._setValue_arrayElement,Ut.prototype._setValue_arrayElement_setNeedsUpdate,Ut.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Ut.prototype._setValue_fromArray,Ut.prototype._setValue_fromArray_setNeedsUpdate,Ut.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var H3=new Float32Array(1);typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"170"}}));typeof window<"u"&&(window.__THREE__?console.warn("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="170");var HN=["hologramCanvas"],gu=class n{constructor(e){this.audioService=e;this.userMood="focused";this.coreGroup=new Hi;this.chakraBladesGroup=new Hi;this.clock=new fu;this.mouse={x:0,y:0,targetX:0,targetY:0};this.state="idle";this.audioIntensity=0;this.animFrameId=0;this.subs=[];this.animate=()=>{this.animFrameId=requestAnimationFrame(this.animate);let e=this.clock.getDelta(),t=this.clock.getElapsedTime();this.mouse.x+=(this.mouse.targetX-this.mouse.x)*.05,this.mouse.y+=(this.mouse.targetY-this.mouse.y)*.05;let i=1;this.state==="thinking"&&(i=3.5),this.state==="executing"&&(i=2.4),this.state==="speaking"&&(i=1.8),this.chakraBladesGroup&&(this.chakraBladesGroup.rotation.z+=.8*e*i),this.chakraRingInner&&(this.chakraRingInner.rotation.x=t*.6*i,this.chakraRingInner.rotation.y=t*.4*i),this.chakraRingMiddle&&(this.chakraRingMiddle.rotation.y=-t*.5*i,this.chakraRingMiddle.rotation.z=t*.3*i),this.chakraRingOuter&&(this.chakraRingOuter.rotation.x=-t*.3*i,this.chakraRingOuter.rotation.z=-t*.5*i);let r=1+Math.sin(t*3)*.05+this.audioIntensity*.4;this.coreGroup.scale.set(r,r,r),this.particleSystem&&(this.particleSystem.rotation.y=t*.12,this.particleSystem.rotation.x=t*.06),this.camera.position.x=this.mouse.x*2.2,this.camera.position.y=this.mouse.y*1.8,this.camera.lookAt(this.scene.position),this.renderer.render(this.scene,this.camera)}}ngAfterViewInit(){this.initThree(),this.subs.push(this.audioService.avatarState$.subscribe(e=>this.setHologramState(e)),this.audioService.audioIntensity$.subscribe(e=>this.audioIntensity=e))}ngOnDestroy(){cancelAnimationFrame(this.animFrameId),this.subs.forEach(e=>e.unsubscribe()),this.renderer&&this.renderer.dispose()}onWindowResize(){if(!this.canvasRef||!this.renderer||!this.camera)return;let e=this.canvasRef.nativeElement,t=e.clientWidth||300,i=e.clientHeight||240;this.camera.aspect=t/i,this.camera.updateProjectionMatrix(),this.renderer.setSize(t,i)}onMouseMove(e){this.mouse.targetX=e.clientX/window.innerWidth*2-1,this.mouse.targetY=-(e.clientY/window.innerHeight)*2+1}initThree(){let e=this.canvasRef.nativeElement,t=e.clientWidth||400,i=e.clientHeight||240;this.scene=new iu,this.camera=new nn(45,t/i,.1,1e3),this.camera.position.z=14,this.renderer=new nu({canvas:e,alpha:!0,antialias:!0}),this.renderer.setSize(t,i),this.renderer.setPixelRatio(Math.min(window.devicePixelRatio,2));let r=new du(16777215,1);this.scene.add(r);let s=new Ea(16766720,3,70);s.position.set(0,2,8),this.scene.add(s);let o=new Ea(61695,2,70);o.position.set(-6,-4,6),this.scene.add(o),this.scene.add(this.coreGroup),this.buildSudarshanaChakra(),this.buildCelestialParticles(),this.animate()}buildSudarshanaChakra(){let e=new au(1.6,2),t=new ds({color:16766720,emissive:10052864,wireframe:!0,roughness:.2,metalness:.8}),i=new Xt(e,t);this.coreGroup.add(i);let r=new lu(.9,0),s=new us({color:61695,transparent:!0,opacity:.85}),o=new Xt(r,s);this.coreGroup.add(o);let a=new po(2.8,.05,16,100),l=new ds({color:16766720,roughness:.1,metalness:.9});this.chakraRingInner=new Xt(a,l),this.coreGroup.add(this.chakraRingInner);let c=new po(3.8,.04,16,100),u=new ds({color:61695,roughness:.1,metalness:.9});this.chakraRingMiddle=new Xt(c,u),this.coreGroup.add(this.chakraRingMiddle);let d=new po(4.8,.06,16,80),h=new us({color:16755200,wireframe:!0,transparent:!0,opacity:.6});this.chakraRingOuter=new Xt(d,h),this.coreGroup.add(this.chakraRingOuter);let f=12;for(let g=0;g<f;g++){let v=g/f*Math.PI*2,m=new su(.18,1.2,4),p=new ds({color:16766720,metalness:.9,roughness:.2}),S=new Xt(m,p);S.position.set(Math.cos(v)*3.8,Math.sin(v)*3.8,0),S.rotation.z=v-Math.PI/2,this.chakraBladesGroup.add(S)}this.coreGroup.add(this.chakraBladesGroup)}buildCelestialParticles(){let t=new Cn,i=new Float32Array(360*3),r=new Float32Array(360*3),s=new $e(16766720),o=new $e(61695),a=new $e(16733440);for(let c=0;c<360*3;c+=3){let u=5+Math.random()*5.5,d=Math.random()*Math.PI*2,h=Math.acos(Math.random()*2-1);i[c]=u*Math.sin(h)*Math.cos(d),i[c+1]=u*Math.sin(h)*Math.sin(d),i[c+2]=u*Math.cos(h);let f=Math.random(),g=f<.5?s:f<.8?o:a;r[c]=g.r,r[c+1]=g.g,r[c+2]=g.b}t.setAttribute("position",new ln(i,3)),t.setAttribute("color",new ln(r,3));let l=new Ma({size:.14,vertexColors:!0,transparent:!0,opacity:.85});this.particleSystem=new ru(t,l),this.scene.add(this.particleSystem)}setHologramState(e){this.state=e}static{this.\u0275fac=function(t){return new(t||n)(we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-hologram-viewport"]],viewQuery:function(t,i){if(t&1&&Zi(HN,5),t&2){let r;Ji(r=Ki())&&(i.canvasRef=r.first)}},hostBindings:function(t,i){t&1&&Se("resize",function(){return i.onWindowResize()},!1,Yo)("mousemove",function(s){return i.onMouseMove(s)},!1,Yo)},inputs:{userMood:"userMood"},decls:14,vars:0,consts:[["hologramCanvas",""],[1,"hologram-stage"],["id","hologramCanvas"],[1,"hologram-overlay"],[1,"hologram-label"],[1,"fas","fa-sun",2,"color","var(--neon-gold)","margin-right","6px"],[1,"hologram-voice-waves"],[1,"voice-bar"]],template:function(t,i){t&1&&(L(0,"div",1),se(1,"canvas",2,0),L(3,"div",3)(4,"div",4),se(5,"i",5),L(6,"span"),J(7,"ABHI CORE // SUDARSHANA CHAKRA"),U()(),L(8,"div",6),se(9,"div",7)(10,"div",7)(11,"div",7)(12,"div",7)(13,"div",7),U()()())},dependencies:[Ot],styles:[".hologram-stage[_ngcontent-%COMP%]{position:relative;border-radius:10px;border:1px solid var(--border-gold);background:radial-gradient(circle at center,#ffd7001f,#0064b426 40%,#030712f2 85%);box-shadow:inset 0 0 24px #ffd7001a,0 0 15px #00f0ff26;overflow:hidden;display:flex;align-items:center;justify-content:center;width:100%;height:100%;min-height:220px}#hologramCanvas[_ngcontent-%COMP%]{width:100%;height:100%;position:absolute;top:0;left:0}.hologram-overlay[_ngcontent-%COMP%]{position:absolute;bottom:8px;left:12px;right:12px;display:flex;align-items:center;justify-content:space-between;pointer-events:none}.hologram-label[_ngcontent-%COMP%]{font-family:var(--font-celestial);font-size:11px;font-weight:700;color:var(--neon-gold);letter-spacing:2px;text-shadow:0 0 10px rgba(255,215,0,.6);display:flex;align-items:center}.hologram-voice-waves[_ngcontent-%COMP%]{display:flex;gap:3px;align-items:center;height:20px}.voice-bar[_ngcontent-%COMP%]{width:3px;height:6px;background:var(--neon-gold);box-shadow:0 0 6px var(--neon-gold);border-radius:2px;animation:_ngcontent-%COMP%_wave 1.2s infinite ease-in-out}.voice-bar[_ngcontent-%COMP%]:nth-child(2){animation-delay:.15s;background:var(--neon-cyan);box-shadow:0 0 6px var(--neon-cyan)}.voice-bar[_ngcontent-%COMP%]:nth-child(3){animation-delay:.3s;background:var(--neon-gold)}.voice-bar[_ngcontent-%COMP%]:nth-child(4){animation-delay:.45s;background:var(--neon-cyan);box-shadow:0 0 6px var(--neon-cyan)}.voice-bar[_ngcontent-%COMP%]:nth-child(5){animation-delay:.6s;background:var(--neon-gold)}@keyframes _ngcontent-%COMP%_wave{0%,to{height:4px}50%{height:18px}}"]})}};var Fx=(()=>{class n{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,i){this._renderer=t,this._elementRef=i}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(i){return new(i||n)(we(Zo),we(Si))};static \u0275dir=vn({type:n})}return n})(),Lx=(()=>{class n extends Fx{static \u0275fac=(()=>{let t;return function(r){return(t||(t=Ff(n)))(r||n)}})();static \u0275dir=vn({type:n,features:[Ci]})}return n})(),Dm=new Ne("");var zN={provide:Dm,useExisting:Ur(()=>fi),multi:!0};function GN(){let n=tr()?tr().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var WN=new Ne(""),fi=(()=>{class n extends Fx{_compositionMode;_composing=!1;constructor(t,i,r){super(t,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!GN())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(i){return new(i||n)(we(Zo),we(Si),we(WN,8))};static \u0275dir=vn({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&Se("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},standalone:!1,features:[Ko([zN]),Ci]})}return n})();var kx=new Ne(""),Ux=new Ne("");function Vx(n){return n!=null}function Bx(n){return Jo(n)?wr(n):n}function Hx(n){let e={};return n.forEach(t=>{e=t!=null?dt(dt({},e),t):e}),Object.keys(e).length===0?null:e}function zx(n,e){return e.map(t=>t(n))}function jN(n){return!n.validate}function Gx(n){return n.map(e=>jN(e)?e:t=>e.validate(t))}function $N(n){if(!n)return null;let e=n.filter(Vx);return e.length==0?null:function(t){return Hx(zx(t,e))}}function Am(n){return n!=null?$N(Gx(n)):null}function qN(n){if(!n)return null;let e=n.filter(Vx);return e.length==0?null:function(t){let i=zx(t,e).map(Bx);return ad(i).pipe(pn(Hx))}}function Im(n){return n!=null?qN(Gx(n)):null}function Dx(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function XN(n){return n._rawValidators}function YN(n){return n._rawAsyncValidators}function wm(n){return n?Array.isArray(n)?n:[n]:[]}function yu(n,e){return Array.isArray(n)?n.includes(e):n===e}function Ax(n,e){let t=wm(e);return wm(n).forEach(r=>{yu(t,r)||t.push(r)}),t}function Ix(n,e){return wm(e).filter(t=>!yu(n,t))}var _u=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=Am(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=Im(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control&&this.control.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},xo=class extends _u{name;get formDirective(){return null}get path(){return null}},Ra=class extends _u{_parent=null;name=null;valueAccessor=null},xu=class{_cd;constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}},ZN={"[class.ng-untouched]":"isUntouched","[class.ng-touched]":"isTouched","[class.ng-pristine]":"isPristine","[class.ng-dirty]":"isDirty","[class.ng-valid]":"isValid","[class.ng-invalid]":"isInvalid","[class.ng-pending]":"isPending"},lV=Ct(dt({},ZN),{"[class.ng-submitted]":"isSubmitted"}),_r=(()=>{class n extends xu{constructor(t){super(t)}static \u0275fac=function(i){return new(i||n)(we(Ra,2))};static \u0275dir=vn({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&ht("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Ci]})}return n})(),bo=(()=>{class n extends xu{constructor(t){super(t)}static \u0275fac=function(i){return new(i||n)(we(xo,10))};static \u0275dir=vn({type:n,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&ht("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[Ci]})}return n})();var Ca="VALID",vu="INVALID",yo="PENDING",Ta="DISABLED",yr=class{},bu=class extends yr{value;source;constructor(e,t){super(),this.value=e,this.source=t}},Aa=class extends yr{pristine;source;constructor(e,t){super(),this.pristine=e,this.source=t}},Ia=class extends yr{touched;source;constructor(e,t){super(),this.touched=e,this.source=t}},_o=class extends yr{status;source;constructor(e,t){super(),this.status=e,this.source=t}},Cm=class extends yr{source;constructor(e){super(),this.source=e}},Tm=class extends yr{source;constructor(e){super(),this.source=e}};function Wx(n){return(Su(n)?n.validators:n)||null}function JN(n){return Array.isArray(n)?Am(n):n||null}function jx(n,e){return(Su(e)?e.asyncValidators:n)||null}function KN(n){return Array.isArray(n)?Im(n):n||null}function Su(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}function QN(n,e,t){let i=n.controls;if(!(e?Object.keys(i):i).length)throw new De(1e3,"");if(!i[t])throw new De(1001,"")}function eP(n,e,t){n._forEachChild((i,r)=>{if(t[r]===void 0)throw new De(1002,"")})}var Mu=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(e,t){this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return Ti(this.statusReactive)}set status(e){Ti(()=>this.statusReactive.set(e))}_status=Qo(()=>this.statusReactive());statusReactive=$o(void 0);get valid(){return this.status===Ca}get invalid(){return this.status===vu}get pending(){return this.status==yo}get disabled(){return this.status===Ta}get enabled(){return this.status!==Ta}errors;get pristine(){return Ti(this.pristineReactive)}set pristine(e){Ti(()=>this.pristineReactive.set(e))}_pristine=Qo(()=>this.pristineReactive());pristineReactive=$o(!0);get dirty(){return!this.pristine}get touched(){return Ti(this.touchedReactive)}set touched(e){Ti(()=>this.touchedReactive.set(e))}_touched=Qo(()=>this.touchedReactive());touchedReactive=$o(!1);get untouched(){return!this.touched}_events=new fn;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(Ax(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(Ax(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(Ix(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(Ix(e,this._rawAsyncValidators))}hasValidator(e){return yu(this._rawValidators,e)}hasAsyncValidator(e){return yu(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsTouched(Ct(dt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Ia(!0,i))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new Ia(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsDirty(Ct(dt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new Aa(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),this._parent&&!e.onlySelf&&this._parent._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new Aa(!0,i))}markAsPending(e={}){this.status=yo;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new _o(this.status,t)),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.markAsPending(Ct(dt({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Ta,this.errors=null,this._forEachChild(r=>{r.disable(Ct(dt({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new bu(this.value,i)),this._events.next(new _o(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(Ct(dt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=Ca,this._forEachChild(i=>{i.enable(Ct(dt({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(Ct(dt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){this._parent&&!e.onlySelf&&(this._parent.updateValueAndValidity(e),e.skipPristineCheck||this._parent._updatePristine({},t),this._parent._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Ca||this.status===yo)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new bu(this.value,t)),this._events.next(new _o(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.updateValueAndValidity(Ct(dt({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Ta:Ca}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=yo,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1};let i=Bx(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=this._hasOwnPendingAsyncValidator?.emitEvent??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i&&i.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new _o(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new Rt,this.statusChanges=new Rt}_calculateStatus(){return this._allControlsDisabled()?Ta:this.errors?vu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(yo)?yo:this._anyControlsHaveStatus(vu)?vu:Ca}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,this._parent&&!e.onlySelf&&this._parent._updatePristine(e,t),r&&this._events.next(new Aa(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new Ia(this.touched,t)),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,t)}_onDisabledChange=[];_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){Su(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){let t=this._parent&&this._parent.dirty;return!e&&!!t&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=JN(this._rawValidators)}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=KN(this._rawAsyncValidators)}},Eu=class extends Mu{constructor(e,t,i){super(Wx(t),jx(i,t)),this.controls=e,this._initObservables(),this._setUpdateStrategy(t),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(e,t){return this.controls[e]?this.controls[e]:(this.controls[e]=t,t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange),t)}addControl(e,t,i={}){this.registerControl(e,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(e,t={}){this.controls[e]&&this.controls[e]._registerOnCollectionChange(()=>{}),delete this.controls[e],this.updateValueAndValidity({emitEvent:t.emitEvent}),this._onCollectionChange()}setControl(e,t,i={}){this.controls[e]&&this.controls[e]._registerOnCollectionChange(()=>{}),delete this.controls[e],t&&this.registerControl(e,t),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(e){return this.controls.hasOwnProperty(e)&&this.controls[e].enabled}setValue(e,t={}){eP(this,!0,e),Object.keys(e).forEach(i=>{QN(this,!0,i),this.controls[i].setValue(e[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t)}patchValue(e,t={}){e!=null&&(Object.keys(e).forEach(i=>{let r=this.controls[i];r&&r.patchValue(e[i],{onlySelf:!0,emitEvent:t.emitEvent})}),this.updateValueAndValidity(t))}reset(e={},t={}){this._forEachChild((i,r)=>{i.reset(e?e[r]:null,{onlySelf:!0,emitEvent:t.emitEvent})}),this._updatePristine(t,this),this._updateTouched(t,this),this.updateValueAndValidity(t)}getRawValue(){return this._reduceChildren({},(e,t,i)=>(e[i]=t.getRawValue(),e))}_syncPendingControls(){let e=this._reduceChildren(!1,(t,i)=>i._syncPendingControls()?!0:t);return e&&this.updateValueAndValidity({onlySelf:!0}),e}_forEachChild(e){Object.keys(this.controls).forEach(t=>{let i=this.controls[t];i&&e(i,t)})}_setUpControls(){this._forEachChild(e=>{e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(e){for(let[t,i]of Object.entries(this.controls))if(this.contains(t)&&e(i))return!0;return!1}_reduceValue(){let e={};return this._reduceChildren(e,(t,i,r)=>((i.enabled||this.disabled)&&(t[r]=i.value),t))}_reduceChildren(e,t){let i=e;return this._forEachChild((r,s)=>{i=t(i,r,s)}),i}_allControlsDisabled(){for(let e of Object.keys(this.controls))if(this.controls[e].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(e){return this.controls.hasOwnProperty(e)?this.controls[e]:null}};var Rm=new Ne("",{providedIn:"root",factory:()=>Nm}),Nm="always";function tP(n,e){return[...e.path,n]}function $x(n,e,t=Nm){qx(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),iP(n,e),sP(n,e),rP(n,e),nP(n,e)}function Rx(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function nP(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function qx(n,e){let t=XN(n);e.validator!==null?n.setValidators(Dx(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=YN(n);e.asyncValidator!==null?n.setAsyncValidators(Dx(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();Rx(e._rawValidators,r),Rx(e._rawAsyncValidators,r)}function iP(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&Xx(n,e)})}function rP(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&Xx(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function Xx(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function sP(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function oP(n,e){n==null,qx(n,e)}function aP(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function lP(n){return Object.getPrototypeOf(n.constructor)===Lx}function cP(n,e){n._syncPendingControls(),e.forEach(t=>{let i=t.control;i.updateOn==="submit"&&i._pendingChange&&(t.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function uP(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===fi?t=s:lP(s)?i=s:r=s}),r||i||t||null}var dP={provide:xo,useExisting:Ur(()=>ms)},Da=Promise.resolve(),ms=(()=>{class n extends xo{callSetDisabledState;get submitted(){return Ti(this.submittedReactive)}_submitted=Qo(()=>this.submittedReactive());submittedReactive=$o(!1);_directives=new Set;form;ngSubmit=new Rt;options;constructor(t,i,r){super(),this.callSetDisabledState=r,this.form=new Eu({},Am(t),Im(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(t){Da.then(()=>{let i=this._findContainer(t.path);t.control=i.registerControl(t.name,t.control),$x(t.control,t,this.callSetDisabledState),t.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(t)})}getControl(t){return this.form.get(t.path)}removeControl(t){Da.then(()=>{let i=this._findContainer(t.path);i&&i.removeControl(t.name),this._directives.delete(t)})}addFormGroup(t){Da.then(()=>{let i=this._findContainer(t.path),r=new Eu({});oP(r,t),i.registerControl(t.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(t){Da.then(()=>{let i=this._findContainer(t.path);i&&i.removeControl(t.name)})}getFormGroup(t){return this.form.get(t.path)}updateModel(t,i){Da.then(()=>{this.form.get(t.path).setValue(i)})}setValue(t){this.control.setValue(t)}onSubmit(t){return this.submittedReactive.set(!0),cP(this.form,this._directives),this.ngSubmit.emit(t),this.form._events.next(new Cm(this.control)),t?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(t=void 0){this.form.reset(t),this.submittedReactive.set(!1),this.form._events.next(new Tm(this.form))}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(t){return t.pop(),t.length?this.form.get(t):this.form}static \u0275fac=function(i){return new(i||n)(we(kx,10),we(Ux,10),we(Rm,8))};static \u0275dir=vn({type:n,selectors:[["form",3,"ngNoForm","",3,"formGroup",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&Se("submit",function(o){return r.onSubmit(o)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ko([dP]),Ci]})}return n})();function Nx(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function Px(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var fP=class extends Mu{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(e=null,t,i){super(Wx(t),jx(i,t)),this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Su(t)&&(t.nonNullable||t.initialValueIsDefault)&&(Px(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),this._pendingChange=!1}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){Nx(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){Nx(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){Px(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var hP={provide:Ra,useExisting:Ur(()=>ji)},Ox=Promise.resolve(),ji=(()=>{class n extends Ra{_changeDetectorRef;callSetDisabledState;control=new fP;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Rt;constructor(t,i,r,s,o,a){super(),this._changeDetectorRef=o,this.callSetDisabledState=a,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=uP(this,s)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),aP(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective&&this.formDirective.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){$x(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){Ox.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&C_(i);Ox.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?tP(t,this._parent):[t]}static \u0275fac=function(i){return new(i||n)(we(xo,9),we(kx,10),we(Ux,10),we(Dm,10),we(oh,8),we(Rm,8))};static \u0275dir=vn({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Ko([hP]),Ci,Sf]})}return n})();var Mo=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=vn({type:n,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return n})();var pP={provide:Dm,useExisting:Ur(()=>Pm),multi:!0},Pm=(()=>{class n extends Lx{writeValue(t){this.setProperty("value",parseFloat(t))}registerOnChange(t){this.onChange=i=>{t(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=Ff(n)))(r||n)}})();static \u0275dir=vn({type:n,selectors:[["input","type","range","formControlName",""],["input","type","range","formControl",""],["input","type","range","ngModel",""]],hostBindings:function(i,r){i&1&&Se("change",function(o){return r.onChange(o.target.value)})("input",function(o){return r.onChange(o.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Ko([pP]),Ci]})}return n})();var mP=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Hr({type:n});static \u0275inj=Vr({})}return n})();var xr=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:Rm,useValue:t.callSetDisabledState??Nm}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=Hr({type:n});static \u0275inj=Vr({imports:[mP]})}return n})();function gP(n,e){if(n&1&&(L(0,"div",57)(1,"div",58)(2,"span"),se(3,"i",59),J(4," USER BIOMETRICS"),U(),L(5,"span",60),J(6),Gr(7,"uppercase"),U()(),L(8,"div",61)(9,"div",62)(10,"span",63),J(11,"STRESS INDEX"),U(),L(12,"div",64)(13,"span",65),J(14),U(),L(15,"div",66),se(16,"div",67),U()()(),L(17,"div",62)(18,"span",63),J(19,"FOCUS SCORE"),U(),L(20,"div",64)(21,"span",68),J(22),U(),L(23,"div",66),se(24,"div",69),U()()()(),L(25,"div",70),se(26,"i",71),L(27,"span"),J(28),U()()()),n&2){let t=kt();Q(5),Oe("ngClass",t.telemetry.emotion.mood),Q(),Dt(Wr(7,13,t.telemetry.emotion.mood)),Q(7),yn("color",t.telemetry.emotion.stress_level>50?"var(--neon-crimson)":"var(--neon-emerald)"),Q(),St(" ",t.telemetry.emotion.stress_level,"% "),Q(2),yn("width",t.telemetry.emotion.stress_level,"%"),ht("danger",t.telemetry.emotion.stress_level>50),Q(6),St("",t.telemetry.emotion.focus_score,"%"),Q(2),yn("width",t.telemetry.emotion.focus_score,"%"),Q(4),Dt(t.telemetry.emotion.recommendation)}}function vP(n,e){if(n&1&&(L(0,"div",72),J(1),U()),n&2){let t=kt();Q(),St(" ",t.macroStatusText," ")}}var Cu=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.telemetry=null;this.appLaunched=new Rt;this.macroTriggered=new Rt;this.volumeLevel=50;this.isMuted=!1;this.brightnessLevel=70;this.macroStatusText=""}ngOnInit(){this.refreshHardwareState()}refreshHardwareState(){this.apiService.getSystemState().subscribe({next:e=>{e&&(e.volume!==void 0&&(this.volumeLevel=e.volume),e.muted!==void 0&&(this.isMuted=e.muted),e.brightness!==void 0&&(this.brightnessLevel=e.brightness))},error:()=>{}})}onVolumeChange(e){this.volumeLevel=Number(e),this.isMuted=!1,this.apiService.setVolume(this.volumeLevel).subscribe()}toggleMute(){this.audioService.playSciFiTone("beep"),this.isMuted=!this.isMuted,this.apiService.setVolume(null,this.isMuted).subscribe()}onBrightnessChange(e){this.brightnessLevel=Number(e),this.apiService.setBrightness(this.brightnessLevel).subscribe()}launchApp(e){this.audioService.playSciFiTone("ack"),this.apiService.launchApp(e).subscribe(),this.appLaunched.emit(e)}runMacro(e){this.audioService.playSciFiTone("boot"),this.macroStatusText=`Executing ${e.toUpperCase()} protocol...`,this.apiService.runMacro(e).subscribe({next:t=>{this.macroStatusText=t.status||`Protocol ${e} engaged.`,this.macroTriggered.emit(e)},error:()=>{this.macroStatusText=`Error running ${e}.`}})}purgeRam(){this.audioService.playSciFiTone("ack"),this.macroStatusText="Purging working set memory...",this.apiService.purgeRam().subscribe({next:e=>{this.macroStatusText=`Optimized ${e.processes_optimized} processes. RAM: ${e.current_memory_percent}%.`},error:()=>{this.macroStatusText="RAM purge completed."}})}static{this.\u0275fac=function(t){return new(t||n)(we(Qt),we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-system-diagnostics"]],inputs:{telemetry:"telemetry"},outputs:{appLaunched:"appLaunched",macroTriggered:"macroTriggered"},decls:113,vars:18,consts:[[1,"cyber-panel"],[1,"panel-header"],[1,"fas","fa-om",2,"color","var(--neon-gold)"],[2,"font-size","10px","color","var(--neon-gold)","font-family","var(--font-hud)"],[1,"panel-body"],["class","biometrics-card",4,"ngIf"],[1,"metric-grid"],[1,"metric-card"],[1,"metric-title"],[1,"fas","fa-microchip"],[1,"metric-val"],[1,"metric-progress"],[1,"progress-bar","cyan"],[1,"fas","fa-memory"],[1,"progress-bar","gold"],[1,"storage-pool-box"],[1,"storage-pool-header"],[1,"fas","fa-book-open",2,"color","var(--neon-gold)","margin-right","4px"],[1,"metric-progress",2,"height","8px"],[1,"progress-bar","green"],[1,"hardware-ctrl-header"],[1,"fas","fa-bolt",2,"color","var(--neon-cyan)"],[1,"ctrl-subtag"],[1,"control-slider-group"],[1,"slider-item"],[1,"slider-label-row"],[1,"fas","fa-volume-high"],[1,"slider-val-badge"],["title","Toggle Audio Mute",1,"btn-mute",3,"click"],[1,"fas",3,"ngClass"],["type","range","min","0","max","100",1,"cyber-range","gold",3,"ngModelChange","ngModel"],[1,"fas","fa-sun"],["type","range","min","0","max","100",1,"cyber-range","cyan",3,"ngModelChange","ngModel"],[1,"protocols-header"],[1,"fas","fa-fire",2,"color","var(--neon-saffron)"],[1,"btn-mini-action",3,"click"],[1,"fas","fa-broom"],[1,"macro-grid"],[1,"btn-macro",3,"click"],[1,"fas","fa-code"],[1,"fas","fa-shield-halved"],[1,"fas","fa-moon"],["class","macro-alert",4,"ngIf"],[1,"apps-header"],[1,"fas","fa-rocket"],[1,"app-grid"],[1,"btn-app",2,"border-color","rgba(37,211,102,0.4)",3,"click"],[1,"fab","fa-whatsapp",2,"color","#25d366"],[1,"btn-app",2,"border-color","rgba(255,0,0,0.4)",3,"click"],[1,"fab","fa-youtube",2,"color","#ff0000"],[1,"btn-app",3,"click"],[1,"fab","fa-github",2,"color","#fff"],[1,"fab","fa-chrome"],[1,"fas","fa-terminal"],[1,"fas","fa-sticky-note"],[1,"fas","fa-calculator"],[1,"fas","fa-folder-open"],[1,"biometrics-card"],[1,"bio-header"],[1,"fas","fa-brain",2,"color","var(--neon-gold)"],[1,"bio-mood-pill",3,"ngClass"],[1,"bio-grid"],[1,"bio-stat"],[1,"bio-label"],[1,"bio-val-row"],[1,"bio-num"],[1,"bio-mini-progress"],[1,"bio-bar"],[1,"bio-num",2,"color","var(--neon-cyan)"],[1,"bio-bar","cyan"],[1,"bio-recommendation"],[1,"fas","fa-circle-info",2,"color","var(--neon-cyan)","margin-right","4px"],[1,"macro-alert"]],template:function(t,i){t&1&&(L(0,"section",0)(1,"div",1)(2,"span"),se(3,"i",2),J(4," LAKSHMI & INDRA CONTROLS"),U(),L(5,"span",3),J(6,"DIVINE TELEMETRY"),U()(),L(7,"div",4),Tt(8,gP,29,15,"div",5),L(9,"div",6)(10,"div",7)(11,"div",8)(12,"span"),J(13,"CPU Compute"),U(),se(14,"i",9),U(),L(15,"div",10),J(16),U(),L(17,"div",11),se(18,"div",12),U()(),L(19,"div",7)(20,"div",8)(21,"span"),J(22,"RAM Pool"),U(),se(23,"i",13),U(),L(24,"div",10),J(25),U(),L(26,"div",11),se(27,"div",14),U()()(),L(28,"div",15)(29,"div",16)(30,"span"),se(31,"i",17),J(32," Saraswati 50GB Vault"),U(),L(33,"span"),J(34),U()(),L(35,"div",18),se(36,"div",19),U()(),L(37,"div",20)(38,"span"),se(39,"i",21),J(40," INDRA HARDWARE CONTROLS"),U(),L(41,"span",22),J(42,"NATIVE OS"),U()(),L(43,"div",23)(44,"div",24)(45,"div",25)(46,"label"),se(47,"i",26),J(48," Master Volume"),U(),L(49,"div",27)(50,"span"),J(51),U(),L(52,"button",28),Se("click",function(){return i.toggleMute()}),se(53,"i",29),U()()(),L(54,"input",30),Se("ngModelChange",function(s){return i.onVolumeChange(s)}),U()(),L(55,"div",24)(56,"div",25)(57,"label"),se(58,"i",31),J(59," Display Luminance"),U(),L(60,"div",27)(61,"span"),J(62),U()()(),L(63,"input",32),Se("ngModelChange",function(s){return i.onBrightnessChange(s)}),U()()(),L(64,"div",33)(65,"span"),se(66,"i",34),J(67," HANUMAN SPEED PROTOCOLS"),U(),L(68,"button",35),Se("click",function(){return i.purgeRam()}),se(69,"i",36),J(70," PURGE RAM"),U()(),L(71,"div",37)(72,"button",38),Se("click",function(){return i.runMacro("dev_mode")}),se(73,"i",39),J(74," Dev Mode "),U(),L(75,"button",38),Se("click",function(){return i.runMacro("focus_mode")}),se(76,"i",40),J(77," Focus Mode "),U(),L(78,"button",38),Se("click",function(){return i.runMacro("night_mode")}),se(79,"i",41),J(80," Night Mode "),U()(),Tt(81,vP,2,1,"div",42),L(82,"div",43),se(83,"i",44),J(84," INDRA APP SUMMONERS "),U(),L(85,"div",45)(86,"button",46),Se("click",function(){return i.launchApp("whatsapp")}),se(87,"i",47),J(88," WhatsApp"),U(),L(89,"button",48),Se("click",function(){return i.launchApp("youtube")}),se(90,"i",49),J(91," YouTube"),U(),L(92,"button",50),Se("click",function(){return i.launchApp("github")}),se(93,"i",51),J(94," GitHub"),U(),L(95,"button",50),Se("click",function(){return i.launchApp("chrome")}),se(96,"i",52),J(97," Chrome"),U(),L(98,"button",50),Se("click",function(){return i.launchApp("vscode")}),se(99,"i",39),J(100," VS Code"),U(),L(101,"button",50),Se("click",function(){return i.launchApp("terminal")}),se(102,"i",53),J(103," Terminal"),U(),L(104,"button",50),Se("click",function(){return i.launchApp("notepad")}),se(105,"i",54),J(106," Notepad"),U(),L(107,"button",50),Se("click",function(){return i.launchApp("calc")}),se(108,"i",55),J(109," Calculator"),U(),L(110,"button",50),Se("click",function(){return i.launchApp("explorer")}),se(111,"i",56),J(112," Explorer"),U()()()()),t&2&&(Q(8),Oe("ngIf",i.telemetry==null?null:i.telemetry.emotion),Q(8),St("",(i.telemetry==null||i.telemetry.cpu==null?null:i.telemetry.cpu.percent)||0,"%"),Q(2),yn("width",(i.telemetry==null||i.telemetry.cpu==null?null:i.telemetry.cpu.percent)||0,"%"),Q(7),St("",(i.telemetry==null||i.telemetry.memory==null?null:i.telemetry.memory.percent)||0,"%"),Q(2),yn("width",(i.telemetry==null||i.telemetry.memory==null?null:i.telemetry.memory.percent)||0,"%"),Q(7),St("",(i.telemetry==null||i.telemetry.storage_pool_50gb==null?null:i.telemetry.storage_pool_50gb.used_mb)||0," MB / 50 GB"),Q(2),yn("width",(i.telemetry==null||i.telemetry.storage_pool_50gb==null?null:i.telemetry.storage_pool_50gb.used_percentage)||0,"%"),Q(15),Dt(i.isMuted?"MUTED":i.volumeLevel+"%"),Q(),ht("muted",i.isMuted),Q(),Oe("ngClass",i.isMuted?"fa-volume-xmark":"fa-volume-high"),Q(),Oe("ngModel",i.volumeLevel),Q(8),St("",i.brightnessLevel,"%"),Q(),Oe("ngModel",i.brightnessLevel),Q(18),Oe("ngIf",i.macroStatusText))},dependencies:[Ot,qn,Xn,jr,xr,fi,Pm,_r,ji],styles:["[_nghost-%COMP%]{display:block;height:100%}.cyber-panel[_ngcontent-%COMP%]{background:var(--bg-panel);border:1px solid var(--border-gold);border-radius:10px;display:flex;flex-direction:column;backdrop-filter:blur(14px);box-shadow:0 4px 20px #0009;position:relative;overflow:hidden;height:100%}.panel-header[_ngcontent-%COMP%]{height:38px;background:#ffd70014;border-bottom:1px solid var(--border-gold);display:flex;align-items:center;justify-content:space-between;padding:0 12px;font-family:var(--font-celestial);font-size:12px;font-weight:700;letter-spacing:1.5px;color:var(--neon-gold);text-transform:uppercase;flex-shrink:0}.panel-body[_ngcontent-%COMP%]{flex:1;padding:10px;overflow-y:auto;display:flex;flex-direction:column;gap:10px}.biometrics-card[_ngcontent-%COMP%]{background:#ffd7000f;border:1px solid rgba(255,215,0,.3);border-radius:8px;padding:8px 10px;display:flex;flex-direction:column;gap:6px}.bio-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;font-family:var(--font-hud);font-size:11px;color:var(--neon-gold)}.bio-mood-pill[_ngcontent-%COMP%]{font-size:10px;font-family:var(--font-data);padding:2px 6px;border-radius:4px;background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);font-weight:700}.bio-mood-pill.stressed[_ngcontent-%COMP%]{background:#ec489933;border-color:var(--neon-crimson);color:var(--neon-crimson)}.bio-mood-pill.happy[_ngcontent-%COMP%]{background:#ffd70033;border-color:var(--neon-gold);color:var(--neon-gold)}.bio-mood-pill.tired[_ngcontent-%COMP%]{background:#8b5cf633;border-color:var(--neon-amethyst);color:var(--neon-amethyst)}.bio-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr 1fr;gap:8px}.bio-stat[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:2px}.bio-label[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:10px;color:var(--text-dim)}.bio-val-row[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px}.bio-num[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:13px;min-width:32px}.bio-mini-progress[_ngcontent-%COMP%]{flex:1;height:4px;background:#ffffff1a;border-radius:2px;overflow:hidden}.bio-bar[_ngcontent-%COMP%]{height:100%;background:var(--neon-emerald);transition:width .4s ease}.bio-bar.danger[_ngcontent-%COMP%]{background:var(--neon-crimson)}.bio-bar.cyan[_ngcontent-%COMP%]{background:var(--neon-cyan)}.bio-recommendation[_ngcontent-%COMP%]{font-size:10px;color:#cde3f8;line-height:1.3;border-top:1px solid rgba(255,255,255,.08);padding-top:4px}.metric-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr 1fr;gap:8px}.metric-card[_ngcontent-%COMP%]{background:var(--bg-panel-light);border:1px solid rgba(255,215,0,.2);border-radius:8px;padding:8px 10px}.metric-title[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;display:flex;justify-content:space-between}.metric-val[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:16px;color:var(--neon-gold);margin:3px 0}.metric-progress[_ngcontent-%COMP%]{height:4px;background:#ffffff1a;border-radius:2px;overflow:hidden}.progress-bar[_ngcontent-%COMP%]{height:100%;background:linear-gradient(90deg,var(--neon-blue),var(--neon-cyan));width:0%;transition:width .5s ease}.progress-bar.gold[_ngcontent-%COMP%]{background:linear-gradient(90deg,var(--neon-gold),#ff8800)}.progress-bar.green[_ngcontent-%COMP%]{background:linear-gradient(90deg,#00b060,var(--neon-emerald))}.progress-bar.cyan[_ngcontent-%COMP%]{background:linear-gradient(90deg,#0088ff,var(--neon-cyan))}.storage-pool-box[_ngcontent-%COMP%]{background:#ffd7000a;border:1px solid rgba(255,215,0,.3);border-radius:8px;padding:6px 10px}.storage-pool-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;font-family:var(--font-data);font-size:11px;color:var(--neon-gold);margin-bottom:4px}.hardware-ctrl-header[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:11px;color:var(--neon-cyan);letter-spacing:1px;display:flex;justify-content:space-between;align-items:center}.ctrl-subtag[_ngcontent-%COMP%]{font-size:9px;background:#00f0ff26;border:1px solid var(--neon-cyan);padding:1px 5px;border-radius:3px;color:var(--neon-cyan)}.control-slider-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:8px;background:#0f193099;border:1px solid rgba(0,240,255,.25);border-radius:8px;padding:8px 10px}.slider-item[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:4px}.slider-item[_ngcontent-%COMP%]   .slider-label-row[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;font-family:var(--font-data);font-size:12px;color:var(--text-main)}.slider-item[_ngcontent-%COMP%]   .slider-label-row[_ngcontent-%COMP%]   label[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px}.slider-item[_ngcontent-%COMP%]   .slider-val-badge[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px;font-family:var(--font-hud);font-size:11px;color:var(--neon-gold)}.slider-item[_ngcontent-%COMP%]   .btn-mute[_ngcontent-%COMP%]{background:#ffd70026;border:1px solid var(--neon-gold);color:var(--neon-gold);border-radius:4px;padding:2px 6px;cursor:pointer;font-size:10px;transition:all .2s}.slider-item[_ngcontent-%COMP%]   .btn-mute[_ngcontent-%COMP%]:hover{background:var(--neon-gold);color:#000}.slider-item[_ngcontent-%COMP%]   .btn-mute.muted[_ngcontent-%COMP%]{background:#ec489933;border-color:var(--neon-crimson);color:var(--neon-crimson)}.slider-item[_ngcontent-%COMP%]   .cyber-range[_ngcontent-%COMP%]{width:100%;cursor:pointer;height:6px;border-radius:3px;appearance:none;background:#ffffff26;outline:none}.slider-item[_ngcontent-%COMP%]   .cyber-range.gold[_ngcontent-%COMP%]::-webkit-slider-thumb{appearance:none;width:16px;height:16px;border-radius:50%;background:var(--neon-gold);box-shadow:0 0 8px var(--neon-gold);cursor:pointer}.slider-item[_ngcontent-%COMP%]   .cyber-range.cyan[_ngcontent-%COMP%]::-webkit-slider-thumb{appearance:none;width:16px;height:16px;border-radius:50%;background:var(--neon-cyan);box-shadow:0 0 8px var(--neon-cyan);cursor:pointer}.protocols-header[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:11px;color:var(--neon-saffron);letter-spacing:1px;display:flex;justify-content:space-between;align-items:center}.macro-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:4px}.btn-macro[_ngcontent-%COMP%]{background:#ff55001f;border:1px solid rgba(255,85,0,.3);border-radius:6px;color:#fff;padding:6px 4px;font-size:10px;font-family:var(--font-data);cursor:pointer;display:flex;align-items:center;justify-content:center;gap:4px;transition:all .2s}.btn-macro[_ngcontent-%COMP%]:hover{background:#ff55004d;border-color:var(--neon-saffron);color:var(--neon-gold)}.btn-mini-action[_ngcontent-%COMP%]{background:#ff550026;border:1px solid var(--neon-saffron);color:var(--neon-saffron);border-radius:4px;padding:2px 8px;font-size:9px;font-family:var(--font-hud);cursor:pointer}.btn-mini-action[_ngcontent-%COMP%]:hover{background:var(--neon-saffron);color:#fff}.macro-alert[_ngcontent-%COMP%]{font-size:10px;font-family:var(--font-data);color:var(--neon-gold);background:#ffd70014;border:1px solid rgba(255,215,0,.3);padding:4px 6px;border-radius:4px}.apps-header[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:11px;color:var(--neon-cyan);letter-spacing:1px}.app-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(3,1fr);gap:4px}.btn-app[_ngcontent-%COMP%]{background:#00f0ff14;border:1px solid rgba(0,240,255,.25);border-radius:6px;color:#fff;padding:6px 3px;font-size:10px;font-family:var(--font-data);display:flex;flex-direction:column;align-items:center;gap:3px;cursor:pointer;transition:all .2s}.btn-app[_ngcontent-%COMP%]:hover{background:#00f0ff40;border-color:var(--neon-cyan);transform:translateY(-1px);box-shadow:0 0 10px #00f0ff4d}"]})}};var yP=["chatScroll"];function _P(n,e){if(n&1&&(L(0,"div",20),se(1,"img",21),U()),n&2){let t=kt().$implicit;Q(),Oe("src",t.imagePreview,Ly)}}function xP(n,e){if(n&1&&(L(0,"div",22),se(1,"i",23),L(2,"span"),J(3,"Deity Action: "),L(4,"strong"),J(5),U()()()),n&2){let t=kt().$implicit;Q(5),Dt(t.toolCall.tool)}}function bP(n,e){if(n&1&&(L(0,"div",14)(1,"div",15),se(2,"i",16),L(3,"span"),J(4),Gr(5,"uppercase"),U()(),L(6,"div",17),J(7),U(),Tt(8,_P,2,1,"div",18)(9,xP,6,1,"div",19),U()),n&2){let t=e.$implicit;Oe("ngClass",t.role==="user"?"user":"abhi"),Q(2),Oe("ngClass",t.role==="user"?"fa-user":"fa-sun"),Q(2),Dt(Wr(5,6,t.sender)),Q(3),Dt(t.content),Q(),Oe("ngIf",t.imagePreview),Q(),Oe("ngIf",t.toolCall)}}var Na=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.selectedAgent="abhi";this.agentResponse=new Rt;this.messages=[{sender:"ABHI",role:"assistant",content:"I am ABHI, your Supreme Cognitive Orchestrator. The Puranic deity swarm (Indra, Saraswati, Narada, Hanuman, Lakshmi, Durga, Vishwakarma) stands ready to execute your command with divine precision."}];this.inputText="";this.subs=[];this.subs.push(this.audioService.speechResult$.subscribe(i=>{this.inputText=i,this.sendMessage()}))}ngAfterViewChecked(){this.scrollToBottom()}toggleMic(){this.audioService.isListening?this.audioService.stopListening():this.audioService.startListening()}sendMessage(){let e=this.inputText.trim();e&&(this.audioService.playSciFiTone("beep"),this.messages.push({sender:"User",role:"user",content:e}),this.inputText="",this.audioService.avatarState$.next("thinking"),this.apiService.chatWithAbhi(e,this.selectedAgent).subscribe({next:t=>{this.audioService.avatarState$.next("idle");let i=t.response||"Divine task completed with absolute precision.";this.messages.push({sender:"ABHI",role:"assistant",content:i,toolCall:t.tool_call}),this.audioService.speak(i),this.agentResponse.emit(t)},error:t=>{this.audioService.avatarState$.next("idle"),this.audioService.playSciFiTone("error"),this.messages.push({sender:"ABHI",role:"assistant",content:`Celestial anomaly encountered: ${t.message}`})}}))}addSystemMessage(e,t,i=null){this.messages.push({sender:e,role:"assistant",content:t,imagePreview:i})}scrollToBottom(){try{this.chatScrollContainer&&(this.chatScrollContainer.nativeElement.scrollTop=this.chatScrollContainer.nativeElement.scrollHeight)}catch{}}static{this.\u0275fac=function(t){return new(t||n)(we(Qt),we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-chat-console"]],viewQuery:function(t,i){if(t&1&&Zi(yP,5),t&2){let r;Ji(r=Ki())&&(i.chatScrollContainer=r.first)}},inputs:{selectedAgent:"selectedAgent"},outputs:{agentResponse:"agentResponse"},decls:19,vars:7,consts:[["chatScroll",""],[1,"chat-container"],[1,"panel-header"],[1,"panel-title"],[1,"fas","fa-om",2,"color","var(--neon-gold)","margin-right","6px"],[1,"active-agent-badge"],[1,"chat-messages"],["class","chat-bubble",3,"ngClass",4,"ngFor","ngForOf"],[1,"chat-input-bar",3,"ngSubmit"],["type","button","title","Voice Dictation",1,"btn-mic",3,"click"],[1,"fas","fa-microphone"],["type","text","name","inputText","placeholder","Ask Abhi or command deities (e.g. 'Set volume 60', 'Launch VS Code', 'Purge RAM', 'Search Saraswati vault')...","autocomplete","off",1,"chat-input",3,"ngModelChange","ngModel"],["type","submit",1,"btn-cyber"],[1,"fas","fa-paper-plane"],[1,"chat-bubble",3,"ngClass"],[1,"bubble-sender"],[1,"fas",2,"margin-right","4px",3,"ngClass"],[1,"bubble-body"],["style","margin-top:8px;",4,"ngIf"],["class","tool-chip",4,"ngIf"],[2,"margin-top","8px"],[2,"max-width","100%","border-radius","6px","border","1px solid var(--neon-gold)",3,"src"],[1,"tool-chip"],[1,"fas","fa-bolt",2,"color","var(--neon-gold)"]],template:function(t,i){if(t&1){let r=Fn();L(0,"div",1)(1,"div",2)(2,"div",3),se(3,"i",4),L(4,"span"),J(5,"ABHI COGNITIVE SUTRADHAR"),U()(),L(6,"span",5),J(7),Gr(8,"uppercase"),U()(),L(9,"div",6,0),Tt(11,bP,10,8,"div",7),U(),L(12,"form",8),Se("ngSubmit",function(){return Nt(r),Pt(i.sendMessage())}),L(13,"button",9),Se("click",function(){return Nt(r),Pt(i.toggleMic())}),se(14,"i",10),U(),L(15,"input",11),er("ngModelChange",function(o){return Nt(r),zr(i.inputText,o)||(i.inputText=o),Pt(o)}),U(),L(16,"button",12),se(17,"i",13),J(18," TRANSMIT"),U()()()}t&2&&(Q(7),St("INVOKED DEITY: [",Wr(8,5,i.selectedAgent),"]"),Q(4),Oe("ngForOf",i.messages),Q(2),ht("recording",i.audioService.isListening),Q(2),Qi("ngModel",i.inputText))},dependencies:[Ot,qn,kn,Xn,jr,xr,Mo,fi,_r,bo,ji,ms],styles:[".chat-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;height:100%;background:var(--bg-panel);border:1px solid var(--border-gold);border-radius:10px;overflow:hidden;box-shadow:0 4px 20px #0009}.panel-header[_ngcontent-%COMP%]{height:38px;background:#ffd70014;border-bottom:1px solid var(--border-gold);display:flex;align-items:center;justify-content:space-between;padding:0 14px;font-family:var(--font-celestial);font-size:12px;font-weight:700;color:var(--neon-gold);flex-shrink:0}.panel-title[_ngcontent-%COMP%]{display:flex;align-items:center}.active-agent-badge[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:11px;color:var(--neon-cyan);background:#00f0ff1a;border:1px solid rgba(0,240,255,.3);padding:2px 8px;border-radius:4px}.chat-messages[_ngcontent-%COMP%]{flex:1;padding:12px;overflow-y:auto;display:flex;flex-direction:column;gap:10px}.chat-bubble[_ngcontent-%COMP%]{max-width:85%;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.5;position:relative;word-break:break-word}.chat-bubble.user[_ngcontent-%COMP%]{align-self:flex-end;background:#00f0ff26;border:1px solid var(--neon-cyan);color:#fff;border-bottom-right-radius:1px}.chat-bubble.user[_ngcontent-%COMP%]   .bubble-sender[_ngcontent-%COMP%]{color:var(--neon-cyan)}.chat-bubble.abhi[_ngcontent-%COMP%]{align-self:flex-start;background:#ffd70014;border:1px solid var(--border-gold);color:var(--text-main);border-bottom-left-radius:1px;box-shadow:0 0 12px #ffd70014}.chat-bubble.abhi[_ngcontent-%COMP%]   .bubble-sender[_ngcontent-%COMP%]{color:var(--neon-gold);font-family:var(--font-celestial);font-weight:700}.bubble-sender[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:10px;letter-spacing:1px;margin-bottom:4px;display:flex;align-items:center}.bubble-body[_ngcontent-%COMP%]{white-space:pre-wrap}.tool-chip[_ngcontent-%COMP%]{margin-top:8px;display:inline-flex;align-items:center;gap:6px;background:#ffd7001f;border:1px solid var(--neon-gold);color:var(--neon-gold);border-radius:4px;padding:3px 8px;font-family:var(--font-data);font-size:11px}.chat-input-bar[_ngcontent-%COMP%]{padding:8px 10px;background:#0f1930d9;border-top:1px solid var(--border-gold);display:flex;gap:8px;align-items:center;flex-shrink:0}.btn-mic[_ngcontent-%COMP%]{width:38px;height:38px;border-radius:8px;background:#ffd7001f;border:1px solid var(--neon-gold);color:var(--neon-gold);display:flex;align-items:center;justify-content:center;cursor:pointer;transition:all .2s}.btn-mic[_ngcontent-%COMP%]:hover{background:var(--neon-gold);color:#000;box-shadow:0 0 10px var(--neon-gold)}.btn-mic.recording[_ngcontent-%COMP%]{background:var(--neon-crimson);border-color:var(--neon-crimson);color:#fff;box-shadow:0 0 12px var(--neon-crimson);animation:_ngcontent-%COMP%_mic-pulse 1s infinite alternate}.chat-input[_ngcontent-%COMP%]{flex:1;background:#0006;border:1px solid rgba(255,215,0,.3);border-radius:8px;color:#fff;padding:8px 14px;font-family:var(--font-body);font-size:13px;outline:none;transition:all .2s}.chat-input[_ngcontent-%COMP%]:focus{border-color:var(--neon-gold);box-shadow:0 0 10px #ffd7004d}.chat-input[_ngcontent-%COMP%]::placeholder{color:#fff6;font-size:12px}.btn-cyber[_ngcontent-%COMP%]{background:#ffd70026;border:1px solid var(--neon-gold);color:var(--neon-gold);font-family:var(--font-hud);font-size:11px;letter-spacing:1px;padding:0 16px;height:38px;border-radius:8px;cursor:pointer;transition:all .2s ease;display:inline-flex;align-items:center;gap:6px}.btn-cyber[_ngcontent-%COMP%]:hover{background:var(--neon-gold);color:#000;box-shadow:0 0 12px var(--neon-gold)}@keyframes _ngcontent-%COMP%_mic-pulse{0%{transform:scale(1)}to{transform:scale(1.08)}}"]})}};function MP(n,e){n&1&&se(0,"i",18)}function EP(n,e){if(n&1&&(L(0,"div",19),se(1,"i",20),L(2,"span"),J(3),U()()),n&2){let t=kt().$implicit;Q(3),Dt(t.mantra)}}function SP(n,e){if(n&1&&(L(0,"span",23),J(1),U()),n&2){let t=e.$implicit;Q(),Dt(t)}}function wP(n,e){if(n&1&&(L(0,"div",21),Tt(1,SP,2,1,"span",22),U()),n&2){let t=kt().$implicit;Q(),Oe("ngForOf",t.capabilities)}}function CP(n,e){if(n&1){let t=Fn();L(0,"div",7),Se("click",function(){let r=Nt(t).$implicit,s=kt();return Pt(s.selectAgent(r.id))}),L(1,"div",8)(2,"div",9),se(3,"i",10),U(),L(4,"div",11)(5,"div",12),Tt(6,MP,1,0,"i",13),J(7),U(),L(8,"div",14),J(9),U()(),L(10,"div",15),J(11),Gr(12,"uppercase"),U()(),Tt(13,EP,4,1,"div",16)(14,wP,2,1,"div",17),U()}if(n&2){let t=e.$implicit,i=kt();yn("--agent-color",t.avatar_color||"#ffd700"),ht("selected",i.selectedAgentId===t.id),Q(2),yn("color",t.avatar_color||"#ffd700"),Q(),Oe("ngClass",i.getDeityIcon(t.id)),Q(2),yn("color",t.avatar_color||"#ffd700"),Q(),Oe("ngIf",i.selectedAgentId===t.id),Q(),St(" ",t.name," "),Q(2),Dt(t.deity||t.role),Q(),Oe("ngClass",t.status),Q(),Dt(Wr(12,16,t.status)),Q(2),Oe("ngIf",t.mantra),Q(),Oe("ngIf",t.capabilities)}}var Tu=class n{constructor(e){this.audioService=e;this.agents=[];this.selectedAgentId="abhi";this.agentSelected=new Rt}selectAgent(e){this.audioService.playSciFiTone("ack"),this.agentSelected.emit(e)}getDeityIcon(e){switch(e.toLowerCase()){case"abhi":case"supervisor":return"fa-crown";case"indra":case"os_controller":return"fa-bolt-lightning";case"saraswati":case"storage_agent":return"fa-book-open";case"narada":case"researcher":return"fa-globe";case"hanuman":case"automation_agent":return"fa-fire";case"lakshmi":return"fa-heart";case"durga":return"fa-shield-halved";case"vishwakarma":return"fa-cubes";default:return"fa-user-astronaut"}}static{this.\u0275fac=function(t){return new(t||n)(we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-multi-agent-swarm"]],inputs:{agents:"agents",selectedAgentId:"selectedAgentId"},outputs:{agentSelected:"agentSelected"},decls:10,vars:1,consts:[[1,"agent-swarm-panel"],[1,"swarm-header"],[1,"swarm-title"],[1,"fas","fa-om",2,"color","var(--neon-gold)","margin-right","6px"],[1,"swarm-sub"],[1,"agent-list"],["class","agent-card",3,"selected","--agent-color","click",4,"ngFor","ngForOf"],[1,"agent-card",3,"click"],[1,"agent-card-header"],[1,"agent-icon-badge"],[1,"fas",3,"ngClass"],[1,"agent-meta"],[1,"agent-name"],["class","fas fa-check-circle","style","color:var(--neon-gold); margin-right:4px;",4,"ngIf"],[1,"agent-deity-role"],[1,"agent-status-tag",3,"ngClass"],["class","agent-mantra",4,"ngIf"],["class","capabilities-list",4,"ngIf"],[1,"fas","fa-check-circle",2,"color","var(--neon-gold)","margin-right","4px"],[1,"agent-mantra"],[1,"fas","fa-sun",2,"font-size","9px","margin-right","4px"],[1,"capabilities-list"],["class","cap-pill",4,"ngFor","ngForOf"],[1,"cap-pill"]],template:function(t,i){t&1&&(L(0,"div",0)(1,"div",1)(2,"div",2),se(3,"i",3),L(4,"span"),J(5,"PURANIC DEITY SWARM"),U()(),L(6,"span",4),J(7,"CLICK TO INVOKE"),U()(),L(8,"div",5),Tt(9,CP,15,18,"div",6),U()()),t&2&&(Q(9),Oe("ngForOf",i.agents))},dependencies:[Ot,qn,kn,Xn,jr],styles:['.agent-swarm-panel[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:8px;height:100%}.swarm-header[_ngcontent-%COMP%]{font-family:var(--font-hud);font-size:11px;color:var(--neon-gold);margin-bottom:4px;letter-spacing:1px;display:flex;justify-content:space-between;align-items:center}.swarm-title[_ngcontent-%COMP%]{display:flex;align-items:center;font-family:var(--font-celestial);font-size:13px;font-weight:700}.swarm-sub[_ngcontent-%COMP%]{font-size:9px;color:var(--text-dim);font-family:var(--font-data)}.agent-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:8px;overflow-y:auto;max-height:580px;padding-right:4px}.agent-card[_ngcontent-%COMP%]{background:var(--bg-panel-light);border:1px solid rgba(255,215,0,.2);border-radius:8px;padding:10px 12px;cursor:pointer;transition:all .25s ease;position:relative;overflow:hidden}.agent-card[_ngcontent-%COMP%]:before{content:"";position:absolute;top:0;left:0;width:3px;height:100%;background:var(--agent-color, var(--neon-gold))}.agent-card[_ngcontent-%COMP%]:hover{background:#ffd70014;border-color:var(--agent-color, var(--neon-gold));transform:translate(3px);box-shadow:0 0 12px #ffd70026}.agent-card.selected[_ngcontent-%COMP%]{border-color:var(--agent-color, var(--neon-gold));background:#ffd7001f;box-shadow:0 0 15px #ffd70040}.agent-card-header[_ngcontent-%COMP%]{display:flex;align-items:center;gap:10px;margin-bottom:4px}.agent-icon-badge[_ngcontent-%COMP%]{width:28px;height:28px;border-radius:6px;background:#00000059;border:1px solid var(--agent-color, var(--neon-gold));display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0}.agent-meta[_ngcontent-%COMP%]{flex:1;min-width:0}.agent-name[_ngcontent-%COMP%]{font-family:var(--font-celestial);font-size:13px;font-weight:700;letter-spacing:.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.agent-deity-role[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:11px;color:var(--text-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.agent-status-tag[_ngcontent-%COMP%]{font-family:var(--font-data);font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;background:#ffffff0d;border:1px solid rgba(255,255,255,.2);color:var(--text-dim);flex-shrink:0}.agent-status-tag.idle[_ngcontent-%COMP%]{color:var(--neon-cyan);border-color:#00f0ff66}.agent-status-tag.active[_ngcontent-%COMP%]{color:var(--neon-gold);border-color:var(--neon-gold);background:#ffd70026}.agent-status-tag.thinking[_ngcontent-%COMP%]{color:var(--neon-gold);border-color:var(--neon-gold);background:#ffd70033;animation:_ngcontent-%COMP%_blink 1s infinite alternate}.agent-status-tag.executing[_ngcontent-%COMP%]{color:var(--neon-emerald);border-color:var(--neon-emerald);background:#10b98133}.agent-mantra[_ngcontent-%COMP%]{font-family:var(--font-celestial);font-size:10px;color:var(--text-gold);background:#ffd7000d;border-left:2px solid var(--neon-gold);padding:3px 6px;border-radius:2px;margin-bottom:6px;display:flex;align-items:center}.capabilities-list[_ngcontent-%COMP%]{display:flex;flex-wrap:wrap;gap:4px}.cap-pill[_ngcontent-%COMP%]{font-size:9px;font-family:var(--font-data);background:#00f0ff14;border:1px solid rgba(0,240,255,.2);color:var(--text-main);padding:1px 6px;border-radius:3px}@keyframes _ngcontent-%COMP%_blink{0%{opacity:.5}to{opacity:1}}']})}};function TP(n,e){if(n&1&&(L(0,"div",19),J(1),U()),n&2){let t=kt();Q(),St(" ",t.statusMessage," ")}}function DP(n,e){if(n&1&&(L(0,"div",22)(1,"div",23),J(2),U(),L(3,"div",24),J(4),U(),L(5,"div",25),J(6),U()()),n&2){let t=e.$implicit;Q(2),rh("",t.file_name," (",t.category,")"),Q(2),Dt(t.file_path),Q(2),Dt(t.summary||"Indexed document")}}function AP(n,e){if(n&1&&(L(0,"div",20),Tt(1,DP,7,4,"div",21),U()),n&2){let t=kt();Q(),Oe("ngForOf",t.searchResults)}}function IP(n,e){n&1&&(L(0,"div",26),J(1," No files currently in 50GB storage pool. "),U())}function RP(n,e){if(n&1&&(L(0,"div",27)(1,"div"),se(2,"i",28),L(3,"span"),J(4),U()(),L(5,"div",29),J(6),U()()),n&2){let t=e.$implicit;Q(2),Oe("ngClass",t.is_directory?"fa-folder":"fa-file-code"),Q(2),Dt(t.name),Q(2),St(" ",t.size_formatted," ")}}var Du=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.files=[];this.searchResults=[];this.searchQuery="";this.indexDirPath="";this.statusMessage=""}ngOnInit(){this.loadFiles()}loadFiles(){this.apiService.listStorageFiles().subscribe(e=>{this.files=e.files||[]})}onFileSelected(e){let t=e.target.files[0];t&&(this.statusMessage=`Uploading ${t.name}...`,this.apiService.uploadStorageFile(t).subscribe({next:i=>{this.audioService.playSciFiTone("ack"),this.statusMessage=`Saved ${i.filename} (${i.size} bytes).`,this.loadFiles()},error:i=>{this.audioService.playSciFiTone("error"),this.statusMessage=`Upload error: ${i.message}`}}))}searchKnowledge(){let e=this.searchQuery.trim();e&&(this.audioService.playSciFiTone("beep"),this.apiService.searchKnowledge(e).subscribe(t=>{this.searchResults=t.results||[]}))}indexDirectory(){let e=this.indexDirPath.trim();e&&(this.audioService.playSciFiTone("ack"),this.statusMessage=`Scanning and indexing ${e}...`,this.apiService.indexDirectory(e).subscribe({next:t=>{this.statusMessage=`Indexed ${t.indexed_count} files into SQLite knowledge base.`,this.loadFiles()},error:t=>{this.statusMessage=`Index error: ${t.message}`}}))}static{this.\u0275fac=function(t){return new(t||n)(we(Qt),we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-storage-vault"]],decls:29,vars:6,consts:[["fileInput",""],[1,"storage-vault-container"],[1,"dropzone",3,"click"],[1,"fas","fa-cloud-arrow-up",2,"font-size","18px","color","var(--neon-cyan)","margin-bottom","4px"],["type","file",2,"display","none",3,"change"],["class","status-alert",4,"ngIf"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)","margin","6px 0"],[1,"fas","fa-search"],[1,"search-form",3,"ngSubmit"],["type","text","name","searchQuery","placeholder","Search indexed files...",1,"chat-input",3,"ngModelChange","ngModel"],["type","submit",1,"btn-cyber"],["class","search-results-list",4,"ngIf"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)","margin","8px 0 4px 0"],[1,"fas","fa-folder-plus"],["type","text","name","indexDirPath","placeholder","Path (e.g. C:\\Users\\AYYAPPA RAYUDU\\Documents)",1,"chat-input",3,"ngModelChange","ngModel"],[1,"fas","fa-server"],[1,"file-list"],["style","color:var(--text-dim); font-size:11px; padding:6px;",4,"ngIf"],["class","file-item",4,"ngFor","ngForOf"],[1,"status-alert"],[1,"search-results-list"],["class","search-item",4,"ngFor","ngForOf"],[1,"search-item"],[1,"search-name"],[1,"search-path"],[1,"search-summary"],[2,"color","var(--text-dim)","font-size","11px","padding","6px"],[1,"file-item"],[1,"fas",2,"color","var(--neon-cyan)","margin-right","6px",3,"ngClass"],[2,"color","var(--text-dim)","font-size","11px"]],template:function(t,i){if(t&1){let r=Fn();L(0,"div",1)(1,"div",2),Se("click",function(){Nt(r);let o=M_(6);return Pt(o.click())}),se(2,"i",3),L(3,"div"),J(4,"Click or drop file to upload to 50GB Pool"),U(),L(5,"input",4,0),Se("change",function(o){return Nt(r),Pt(i.onFileSelected(o))}),U()(),Tt(7,TP,2,1,"div",5),L(8,"div",6),se(9,"i",7),J(10," SEARCH LAPTOP KNOWLEDGE "),U(),L(11,"form",8),Se("ngSubmit",function(){return Nt(r),Pt(i.searchKnowledge())}),L(12,"input",9),er("ngModelChange",function(o){return Nt(r),zr(i.searchQuery,o)||(i.searchQuery=o),Pt(o)}),U(),L(13,"button",10),se(14,"i",7),U()(),Tt(15,AP,2,1,"div",11),L(16,"div",12),se(17,"i",13),J(18," DEEP DIRECTORY INDEXER "),U(),L(19,"form",8),Se("ngSubmit",function(){return Nt(r),Pt(i.indexDirectory())}),L(20,"input",14),er("ngModelChange",function(o){return Nt(r),zr(i.indexDirPath,o)||(i.indexDirPath=o),Pt(o)}),U(),L(21,"button",10),J(22,"INDEX"),U()(),L(23,"div",12),se(24,"i",15),J(25," 50GB POOL EXPLORER "),U(),L(26,"div",16),Tt(27,IP,2,0,"div",17)(28,RP,7,3,"div",18),U()()}t&2&&(Q(7),Oe("ngIf",i.statusMessage),Q(5),Qi("ngModel",i.searchQuery),Q(3),Oe("ngIf",i.searchResults.length>0),Q(5),Qi("ngModel",i.indexDirPath),Q(7),Oe("ngIf",i.files.length===0),Q(),Oe("ngForOf",i.files))},dependencies:[Ot,qn,kn,Xn,xr,Mo,fi,_r,bo,ji,ms],styles:[".storage-vault-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:6px}.dropzone[_ngcontent-%COMP%]{border:1px dashed var(--border-cyan);border-radius:6px;padding:10px;text-align:center;background:#00f0ff08;cursor:pointer;transition:all .2s;font-family:var(--font-data);font-size:12px;color:var(--text-dim)}.dropzone[_ngcontent-%COMP%]:hover{background:#00f0ff14;border-color:var(--neon-cyan);color:#fff}.status-alert[_ngcontent-%COMP%]{background:#00ff9d1a;border:1px solid var(--neon-green);border-radius:4px;padding:4px 8px;font-family:var(--font-data);font-size:11px;color:var(--neon-green)}.search-form[_ngcontent-%COMP%]{display:flex;gap:6px;margin-bottom:6px}.chat-input[_ngcontent-%COMP%]{flex:1;background:#00f0ff0d;border:1px solid var(--border-cyan);border-radius:6px;padding:6px 10px;color:#fff;font-family:var(--font-body);font-size:12px;outline:none}.chat-input[_ngcontent-%COMP%]:focus{border-color:var(--neon-cyan)}.search-results-list[_ngcontent-%COMP%]{max-height:110px;overflow-y:auto;display:flex;flex-direction:column;gap:6px;margin-bottom:8px}.search-item[_ngcontent-%COMP%]{background:#00f0ff0f;border:1px solid rgba(0,240,255,.2);border-radius:4px;padding:6px}.search-name[_ngcontent-%COMP%]{color:var(--neon-cyan);font-weight:700;font-size:12px}.search-path[_ngcontent-%COMP%]{color:var(--text-dim);font-size:10px}.search-summary[_ngcontent-%COMP%]{font-size:11px;color:#cde3f8;margin-top:2px}.file-list[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:4px;max-height:140px;overflow-y:auto}.file-item[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;padding:6px 8px;background:#00f0ff0a;border:1px solid rgba(0,240,255,.1);border-radius:4px;font-family:var(--font-data);font-size:12px}.file-item[_ngcontent-%COMP%]:hover{background:#00f0ff1a}.btn-cyber[_ngcontent-%COMP%]{background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);font-family:var(--font-hud);font-size:11px;padding:6px 10px;border-radius:4px;cursor:pointer}.btn-cyber[_ngcontent-%COMP%]:hover{background:var(--neon-cyan);color:#000}"]})}};function NP(n,e){if(n&1){let t=Fn();L(0,"tr")(1,"td"),J(2),U(),L(3,"td",10),J(4),U(),L(5,"td",11),J(6),U(),L(7,"td",7),J(8),U(),L(9,"td",8)(10,"button",12),Se("click",function(){let r=Nt(t).$implicit,s=kt();return Pt(s.kill(r.pid))}),J(11,"KILL"),U()()()}if(n&2){let t=e.$implicit;Q(2),Dt(t.pid),Q(),Oe("title",t.name),Q(),Dt(t.name),Q(2),St("",t.cpu_percent,"%"),Q(2),St("",t.memory_percent,"%")}}var Au=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.processes=[]}ngOnInit(){this.refresh()}refresh(){this.apiService.getProcesses(25).subscribe(e=>{this.processes=e.processes||[]})}kill(e){confirm(`Terminate process PID ${e}?`)&&(this.audioService.playSciFiTone("ack"),this.apiService.killProcess(e).subscribe(()=>this.refresh()))}static{this.\u0275fac=function(t){return new(t||n)(we(Qt),we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-process-manager"]],decls:23,vars:1,consts:[[1,"process-manager-container"],[1,"process-header"],[2,"font-family","var(--font-hud)","font-size","11px","color","var(--neon-cyan)"],[1,"btn-cyber",2,"padding","2px 8px","font-size","10px",3,"click"],[1,"fas","fa-sync"],[1,"process-table-scroll"],[1,"proc-table"],[2,"text-align","right"],[2,"text-align","center"],[4,"ngFor","ngForOf"],[1,"proc-name",3,"title"],[2,"text-align","right","color","var(--neon-gold)"],[1,"btn-cyber","danger",2,"padding","2px 6px","font-size","9px",3,"click"]],template:function(t,i){t&1&&(L(0,"div",0)(1,"div",1)(2,"span",2),J(3,"ACTIVE PROCESSES"),U(),L(4,"button",3),Se("click",function(){return i.refresh()}),se(5,"i",4),J(6," REFRESH "),U()(),L(7,"div",5)(8,"table",6)(9,"thead")(10,"tr")(11,"th"),J(12,"PID"),U(),L(13,"th"),J(14,"NAME"),U(),L(15,"th",7),J(16,"CPU%"),U(),L(17,"th",7),J(18,"MEM%"),U(),L(19,"th",8),J(20,"ACTION"),U()()(),L(21,"tbody"),Tt(22,NP,12,5,"tr",9),U()()()()),t&2&&(Q(22),Oe("ngForOf",i.processes))},dependencies:[Ot,kn],styles:[".process-manager-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;gap:8px}.process-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center}.process-table-scroll[_ngcontent-%COMP%]{max-height:380px;overflow-y:auto}.proc-table[_ngcontent-%COMP%]{width:100%;border-collapse:collapse;font-size:11px;font-family:var(--font-data)}.proc-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]{text-align:left;padding:4px;color:var(--neon-cyan);border-bottom:1px solid rgba(0,240,255,.2)}.proc-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]{padding:4px;border-bottom:1px solid rgba(255,255,255,.05)}.proc-name[_ngcontent-%COMP%]{max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.btn-cyber[_ngcontent-%COMP%]{background:#00f0ff26;border:1px solid var(--neon-cyan);color:var(--neon-cyan);font-family:var(--font-hud);border-radius:4px;cursor:pointer}.btn-cyber.danger[_ngcontent-%COMP%]{border-color:var(--neon-red);color:var(--neon-red);background:#ff336626}.btn-cyber.danger[_ngcontent-%COMP%]:hover{background:var(--neon-red);color:#fff}"]})}};var PP=["termScroll"];function OP(n,e){if(n&1&&(L(0,"div"),J(1),U()),n&2){let t=e.$implicit;yn("color",t.isError?"var(--neon-red)":"#a8d5ff"),Q(),St(" ",t.text," ")}}var Iu=class n{constructor(e,t){this.apiService=e;this.audioService=t;this.commandInput="";this.terminalLines=[{text:"JARVIS PowerShell Direct Console [Win32 Native Engine]"},{text:"Type any PowerShell command and hit Enter..."}]}execute(){let e=this.commandInput.trim();e&&(this.audioService.playSciFiTone("beep"),this.terminalLines.push({text:`PS > ${e}`}),this.commandInput="",this.audioService.avatarState$.next("executing"),this.apiService.executeTerminal(e).subscribe({next:t=>{this.audioService.avatarState$.next("idle"),t.stdout&&this.terminalLines.push({text:t.stdout}),t.stderr&&this.terminalLines.push({text:`ERROR: ${t.stderr}`,isError:!0}),this.terminalLines.push({text:`[Process exit: ${t.exit_code} | ${t.duration_ms}ms]`}),this.scrollToBottom()},error:t=>{this.audioService.avatarState$.next("idle"),this.terminalLines.push({text:`EXECUTION FAILURE: ${t.message}`,isError:!0}),this.scrollToBottom()}}))}scrollToBottom(){setTimeout(()=>{this.termScrollContainer&&(this.termScrollContainer.nativeElement.scrollTop=this.termScrollContainer.nativeElement.scrollHeight)},50)}static{this.\u0275fac=function(t){return new(t||n)(we(Qt),we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-terminal-runner"]],viewQuery:function(t,i){if(t&1&&Zi(PP,5),t&2){let r;Ji(r=Ki())&&(i.termScrollContainer=r.first)}},decls:8,vars:2,consts:[["termScroll",""],[1,"terminal-window"],[1,"terminal-output"],[3,"color",4,"ngFor","ngForOf"],[1,"terminal-input-row",3,"ngSubmit"],["type","text","name","commandInput","placeholder","Get-Process, ipconfig, ls, Get-Service...",3,"ngModelChange","ngModel"]],template:function(t,i){if(t&1){let r=Fn();L(0,"div",1)(1,"div",2,0),Tt(3,OP,2,3,"div",3),U(),L(4,"form",4),Se("ngSubmit",function(){return Nt(r),Pt(i.execute())}),L(5,"span"),J(6,"PS >"),U(),L(7,"input",5),er("ngModelChange",function(o){return Nt(r),zr(i.commandInput,o)||(i.commandInput=o),Pt(o)}),U()()()}t&2&&(Q(3),Oe("ngForOf",i.terminalLines),Q(4),Qi("ngModel",i.commandInput))},dependencies:[Ot,kn,xr,Mo,fi,_r,bo,ji,ms],styles:[".terminal-window[_ngcontent-%COMP%]{background:#02050b;border:1px solid rgba(0,240,255,.25);border-radius:6px;height:250px;display:flex;flex-direction:column;font-family:Courier New,monospace;font-size:12px}.terminal-output[_ngcontent-%COMP%]{flex:1;padding:8px;overflow-y:auto;color:#a8d5ff;white-space:pre-wrap;line-height:1.35}.terminal-input-row[_ngcontent-%COMP%]{display:flex;border-top:1px solid rgba(0,240,255,.2);padding:4px 8px;background:#00f0ff0d;align-items:center;gap:6px}.terminal-input-row[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{color:var(--neon-green)}.terminal-input-row[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{flex:1;background:transparent;border:none;color:#fff;font-family:inherit;font-size:12px;outline:none}"]})}};var Ru=class n{static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-hud-footer"]],decls:10,vars:0,consts:[[1,"hud-footer"],[1,"fas","fa-shield-halved",2,"color","var(--neon-emerald)"],[1,"fas","fa-book-open",2,"color","var(--neon-gold)"],[1,"fas","fa-sun",2,"color","var(--neon-gold)"]],template:function(t,i){t&1&&(L(0,"footer",0)(1,"div"),se(2,"i",1),J(3," DURGA CELESTIAL SHIELD: ACTIVE"),U(),L(4,"div"),se(5,"i",2),J(6," SARASWATI 50GB VAULT ALLOCATED // MEMORY PERSISTED"),U(),L(7,"div"),se(8,"i",3),J(9," ABHI AIOS // OLLAMA QWEN3:8B LOCAL COGNITION"),U()())},dependencies:[Ot],styles:[".hud-footer[_ngcontent-%COMP%]{display:flex;align-items:center;justify-content:space-between;background:var(--bg-panel);border:1px solid var(--border-gold);border-radius:8px;padding:4px 14px;font-family:var(--font-data);font-size:11px;color:var(--text-dim);flex-wrap:wrap;gap:6px;box-shadow:0 0 10px #0006}"]})}};var Nu=class n{constructor(){this.ws=null;this.reconnectInterval=3e3;this.telemetrySubject=new gi(null);this.telemetry$=this.telemetrySubject.asObservable();this.agentsSubject=new gi([]);this.agents$=this.agentsSubject.asObservable();this.isConnectedSubject=new gi(!1);this.isConnected$=this.isConnectedSubject.asObservable();this.connect()}connect(){let e=window.location.protocol==="https:"?"wss:":"ws:",t=window.location.host||"localhost:8000",i=`${e}//${t}/ws/telemetry`;try{this.ws=new WebSocket(i),this.ws.onopen=()=>{console.log("[JARVIS-WS] Connected to live telemetry stream"),this.isConnectedSubject.next(!0)},this.ws.onmessage=r=>{try{let s=JSON.parse(r.data);s.type==="TELEMETRY_UPDATE"&&(s.telemetry&&this.telemetrySubject.next(s.telemetry),s.agents&&this.agentsSubject.next(s.agents))}catch(s){console.error("[JARVIS-WS] Parse error",s)}},this.ws.onclose=()=>{this.isConnectedSubject.next(!1),setTimeout(()=>this.connect(),this.reconnectInterval)},this.ws.onerror=r=>{this.isConnectedSubject.next(!1)}}catch{setTimeout(()=>this.connect(),this.reconnectInterval)}}static{this.\u0275fac=function(t){return new(t||n)}}static{this.\u0275prov=it({token:n,factory:n.\u0275fac,providedIn:"root"})}};function LP(n,e){if(n&1){let t=Fn();L(0,"div",20)(1,"button",21),Se("click",function(){Nt(t);let r=kt();return Pt(r.setMobileView("diagnostics"))}),se(2,"i",14),J(3," INDRA & LAKSHMI "),U(),L(4,"button",21),Se("click",function(){Nt(t);let r=kt();return Pt(r.setMobileView("chat"))}),se(5,"i",22),J(6," ABHI CORE "),U(),L(7,"button",21),Se("click",function(){Nt(t);let r=kt();return Pt(r.setMobileView("swarm"))}),se(8,"i",23),J(9," DEITY SWARM "),U()()}if(n&2){let t=kt();Q(),ht("active",t.mobileView==="diagnostics"),Q(3),ht("active",t.mobileView==="chat"),Q(3),ht("active",t.mobileView==="swarm")}}var Pu=class n{constructor(e,t,i){this.wsService=e;this.apiService=t;this.audioService=i;this.activeTab="agents";this.mobileView="chat";this.isMobile=!1;this.selectedAgentId="abhi";this.telemetry=null;this.agents=[];this.isConnected=!1;this.netSpeedText="NET: ACTIVE";this.checkScreenSize()}onResize(){this.checkScreenSize()}checkScreenSize(){this.isMobile=window.innerWidth<992}ngOnInit(){this.wsService.isConnected$.subscribe(e=>this.isConnected=e),this.wsService.telemetry$.subscribe(e=>{e&&(this.telemetry=e,e.network&&(this.netSpeedText=`DN: ${e.network.download_speed_kbps} KB/s | UP: ${e.network.upload_speed_kbps} KB/s`))}),this.wsService.agents$.subscribe(e=>{e&&e.length>0&&(this.agents=e)}),this.apiService.getTelemetry().subscribe(e=>this.telemetry=e),this.apiService.getAgentStatus().subscribe(e=>{e.agents&&(this.agents=e.agents)}),this.audioService.playSciFiTone("boot")}setTab(e){this.audioService.playSciFiTone("beep"),this.activeTab=e}setMobileView(e){this.audioService.playSciFiTone("beep"),this.mobileView=e}onAgentSelected(e){this.selectedAgentId=e,this.chatConsole&&this.chatConsole.addSystemMessage("ABHI",`Invoking Puranic deity [${e.toUpperCase()}] for your command.`)}onScreenshotCaptured(e){this.chatConsole&&this.chatConsole.addSystemMessage("ABHI","Hanuman has captured the visual display buffer and preserved it in Saraswati vault.",e)}onAppLaunched(e){this.chatConsole&&this.chatConsole.addSystemMessage("ABHI",`Indra has summoned application ${e.toUpperCase()} on your workstation.`)}onAgentResponse(e){e.agents&&(this.agents=e.agents)}static{this.\u0275fac=function(t){return new(t||n)(we(Nu),we(Qt),we(Bt))}}static{this.\u0275cmp=Vt({type:n,selectors:[["app-root"]],viewQuery:function(t,i){if(t&1&&Zi(Na,5),t&2){let r;Ji(r=Ki())&&(i.chatConsole=r.first)}},hostBindings:function(t,i){t&1&&Se("resize",function(){return i.onResize()},!1,Yo)},decls:34,vars:32,consts:[[1,"abhi-container"],[3,"screenshotCaptured","isConnected","netSpeedText"],["class","mobile-nav-bar",4,"ngIf"],[1,"workspace-grid"],[1,"col-panel","left-col"],[3,"appLaunched","telemetry"],[1,"center-column"],[1,"hologram-wrapper"],[3,"userMood"],[1,"chat-wrapper"],[3,"agentResponse","selectedAgent"],[1,"cyber-panel","right-col"],[1,"tabs-nav"],[1,"tab-btn",3,"click"],[1,"fas","fa-om"],[1,"fas","fa-book-open"],[1,"fas","fa-shield-halved"],[1,"fas","fa-terminal"],[1,"tab-content"],[3,"agentSelected","agents","selectedAgentId"],[1,"mobile-nav-bar"],[1,"mob-btn",3,"click"],[1,"fas","fa-sun"],[1,"fas","fa-users-cog"]],template:function(t,i){t&1&&(L(0,"div",0)(1,"app-hud-header",1),Se("screenshotCaptured",function(s){return i.onScreenshotCaptured(s)}),U(),Tt(2,LP,10,6,"div",2),L(3,"main",3)(4,"div",4)(5,"app-system-diagnostics",5),Se("appLaunched",function(s){return i.onAppLaunched(s)}),U()(),L(6,"section",6)(7,"div",7),se(8,"app-hologram-viewport",8),U(),L(9,"div",9)(10,"app-chat-console",10),Se("agentResponse",function(s){return i.onAgentResponse(s)}),U()()(),L(11,"section",11)(12,"div",12)(13,"button",13),Se("click",function(){return i.setTab("agents")}),se(14,"i",14),J(15," DEITIES "),U(),L(16,"button",13),Se("click",function(){return i.setTab("storage")}),se(17,"i",15),J(18," VAULT "),U(),L(19,"button",13),Se("click",function(){return i.setTab("processes")}),se(20,"i",16),J(21," PROCESSES "),U(),L(22,"button",13),Se("click",function(){return i.setTab("terminal")}),se(23,"i",17),J(24," SHELL "),U()(),L(25,"div",18)(26,"app-multi-agent-swarm",19),Se("agentSelected",function(s){return i.onAgentSelected(s)}),U()(),L(27,"div",18),se(28,"app-storage-vault"),U(),L(29,"div",18),se(30,"app-process-manager"),U(),L(31,"div",18),se(32,"app-terminal-runner"),U()()(),se(33,"app-hud-footer"),U()),t&2&&(Q(),Oe("isConnected",i.isConnected)("netSpeedText",i.netSpeedText),Q(),Oe("ngIf",i.isMobile),Q(),ht("mobile-mode",i.isMobile),Q(),ht("mob-hidden",i.isMobile&&i.mobileView!=="diagnostics"),Q(),Oe("telemetry",i.telemetry),Q(),ht("mob-hidden",i.isMobile&&i.mobileView!=="chat"),Q(2),Oe("userMood",(i.telemetry==null||i.telemetry.emotion==null?null:i.telemetry.emotion.mood)||"focused"),Q(2),Oe("selectedAgent",i.selectedAgentId),Q(),ht("mob-hidden",i.isMobile&&i.mobileView!=="swarm"),Q(2),ht("active",i.activeTab==="agents"),Q(3),ht("active",i.activeTab==="storage"),Q(3),ht("active",i.activeTab==="processes"),Q(3),ht("active",i.activeTab==="terminal"),Q(3),ht("active",i.activeTab==="agents"),Q(),Oe("agents",i.agents)("selectedAgentId",i.selectedAgentId),Q(),ht("active",i.activeTab==="storage"),Q(2),ht("active",i.activeTab==="processes"),Q(2),ht("active",i.activeTab==="terminal"))},dependencies:[Ot,Xn,fc,gu,Cu,Na,Tu,Du,Au,Iu,Ru],styles:[".abhi-container[_ngcontent-%COMP%]{display:grid;grid-template-rows:auto 1fr auto;height:100vh;width:100vw;padding:8px 12px;gap:8px;box-sizing:border-box}.mobile-nav-bar[_ngcontent-%COMP%]{display:flex;gap:6px;background:var(--bg-panel);border:1px solid var(--border-gold);border-radius:8px;padding:4px}.mob-btn[_ngcontent-%COMP%]{flex:1;padding:8px 4px;background:transparent;border:1px solid transparent;border-radius:6px;color:var(--text-dim);font-family:var(--font-hud);font-size:11px;letter-spacing:1px;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;transition:all .2s}.mob-btn.active[_ngcontent-%COMP%]{background:#ffd70033;border-color:var(--neon-gold);color:var(--neon-gold);box-shadow:0 0 8px #ffd70066}.workspace-grid[_ngcontent-%COMP%]{display:grid;grid-template-columns:320px 1fr 350px;gap:8px;height:calc(100vh - 116px);min-height:0;overflow:hidden}.workspace-grid.mobile-mode[_ngcontent-%COMP%]{display:flex;flex-direction:column;height:calc(100vh - 160px)}.col-panel[_ngcontent-%COMP%]{height:100%;min-height:0}.center-column[_ngcontent-%COMP%]{display:grid;grid-template-rows:240px 1fr;gap:8px;height:100%;min-height:0}.hologram-wrapper[_ngcontent-%COMP%], .chat-wrapper[_ngcontent-%COMP%]{height:100%;min-height:0}.cyber-panel[_ngcontent-%COMP%]{background:var(--bg-panel);border:1px solid var(--border-gold);border-radius:10px;display:flex;flex-direction:column;backdrop-filter:blur(14px);box-shadow:0 4px 20px #0009;position:relative;overflow:hidden;height:100%;min-height:0}.tabs-nav[_ngcontent-%COMP%]{display:flex;background:#ffd7000d;border-bottom:1px solid var(--border-gold);flex-shrink:0}.tab-btn[_ngcontent-%COMP%]{flex:1;padding:8px 4px;background:transparent;border:none;color:var(--text-dim);font-family:var(--font-celestial);font-size:11px;font-weight:700;letter-spacing:1px;cursor:pointer;transition:all .2s}.tab-btn.active[_ngcontent-%COMP%]{color:var(--neon-gold);background:#ffd70026;border-bottom:2px solid var(--neon-gold);box-shadow:inset 0 -2px 8px #ffd7004d}.tab-content[_ngcontent-%COMP%]{flex:1;overflow-y:auto;padding:10px;display:none;min-height:0}.tab-content.active[_ngcontent-%COMP%]{display:block}.mob-hidden[_ngcontent-%COMP%]{display:none!important}@media(max-width:1280px){.workspace-grid[_ngcontent-%COMP%]:not(.mobile-mode){grid-template-columns:290px 1fr 310px}}@media(max-width:992px){.abhi-container[_ngcontent-%COMP%]{height:100vh}.center-column[_ngcontent-%COMP%]{grid-template-rows:200px 1fr}}"]})}};yh(Pu,{providers:[bh()]}).catch(n=>console.error(n));
